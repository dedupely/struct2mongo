module struct2mongo;

import mondo;
import bsond;
import std.traits, std.range;

struct Col {
    Collection collection;
    auto insert (S)(S val) {
        collection.insert (val.bson);
    }
    /// Same parameters as Collection.findOne (except the first one).
    /// S (return type) needs to be specified.
    S findOne (S)(
          in Query query = Query.init
        , in QueryFlags flags = QueryFlags.NONE
        , in ReadPrefs readPrefs = null
    ) {
        return collection
            .findOne!BO (query, flags, readPrefs)
            .fromBO!S;
    }
    /// S (return type) needs to be specified.
    auto find (S)(
          in Query query = Query.init
        , in QueryFlags flags = QueryFlags.NONE
        , in ReadPrefs readPrefs = null
    ) {
        import std.algorithm : map;
        return collection
            .find!BO (query, flags, readPrefs)
            .map!(a => a.fromBO!S);
    }

    auto aggregate (S = BO, K)(
          in K aggregate
        , in BsonObject options = BsonObject.init
        , in QueryFlags flags = QueryFlags.NONE
        , in ReadPrefs readPrefs = null
    ) if (is(Unqual!K == BsonArray) || is(Unqual!K == BsonObject)) {
        auto toReturn =
            collection.aggregate (aggregate, options, flags, readPrefs);
        static if (is (S == BO)) {
            return toReturn;
        } else {
            return toReturn.map!(a => a.fromBO!S);
        }
    }

    // From here: Just calls to Mondo's methods.
    auto findOne (
          in Query query = Query.init
        , in QueryFlags flags = QueryFlags.NONE
        , in ReadPrefs readPrefs = null
    ) {
        return collection.findOne (query, flags, readPrefs);
    }
    auto find (
          in Query query = Query.init
        , in QueryFlags flags = QueryFlags.NONE
        , in ReadPrefs readPrefs = null
    ) {
        return collection.find (query, flags, readPrefs);
    }

    alias collection this;
}
unittest {
    Mongo mongo = new Mongo("mongodb://localhost");
    assert (mongo.connected, `Not connected to Mongo`);
    auto collection = Col (mongo [`newBase`][`newCollection`]);
    // Be careful, this deletes the Collection.
    if (collection.exists) collection.drop;
    struct Foo {int a = 4;}
    collection.insert (Foo ());
    assert (collection.findOne!Foo == Foo ());
    assert (collection.find!Foo.front == Foo ());
    collection.insert (Foo (8));
    assert (
        collection
        .aggregate!Foo (BA ([BO(`$match`, BO (`a`, 8))]))
        .front == Foo (8)
    );

    // Test Mondo's methods.
    assert (
        collection
        .aggregate (BA ([BO(`$match`, BO (`a`, 8))]))
        .front [`a`] == 8
    );
    assert (collection.find.array.length == 2);
    assert ((`a` !in collection.findOne ()) || collection.findOne ()[`a`] == 8);
}

// A BsonObject converted to BO it's just itself.
auto bson (BO b) { return b;}

enum MongoKeep;
BO bson (Type)(Type instance) {
    static assert (__traits (isPOD, Type)
        , `bson (instance) is only implemented for POD structs`);

    // If an empty BO constructor is used, it segfaults when appending.
    // Already fixed in master, still not pushed to dub.
    auto toReturn = BO (`a`, `b`);
    toReturn.remove (`a`);
    static foreach (field; FieldNameTuple!Type) { {
        auto instanceField = __traits (getMember, instance, field);
        // Save only the fields with non default values or the ones that
        // have the @MongoKeep UDA.
        if (
            instanceField != __traits(getMember, Type.init, field)
            || hasUDA! (mixin (`Type.` ~ field), MongoKeep)
        ) {
            static if (field == `_id`) {
                auto toInsert = ObjectId (instanceField);
            } else {
                auto toInsert = recursiveBsonArray (instanceField);
            }
            toReturn.append (field, toInsert);
        }
    } }
    return toReturn;
}

unittest {
    struct Foo {
        string a = `Hello`;
        int b = 3;
        int [3] c = [2,3,4];
        @MongoKeep bool d = true;
    }
    // If the default values are used, nothing needs to be saved.
    // Note: This one fails, because internally it hasn't been initted.
    // Should be already fixed on Mondo's master.
    //assert (bson (Foo ()) == BO());
    BO toCompare = BO (`d`, true);
    assert (bson (Foo ()) == toCompare);

    toCompare.append (`b`, 5);
    assert (bson (Foo (`Hello`, 5)) == toCompare);

    struct WithId {
        string _id;
    }
    string customId = `dddddddddddddddddddddddd`;
    auto withId = WithId (customId);
    assert (bson (withId) == BO(`_id`, ObjectId(customId))); 
    
}

auto fromBO (Type) (BO bo) {
    static assert (__traits (isPOD, Type)
        , "fromBO is made for POD structs.\n"
        ~ ` Make sure it's okay to assign to ` ~ Type.stringof
        ~ `'s fields and comment this warning.`);
    alias TypeFields = FieldNameTuple!Type;
    Type toReturn;
    static foreach (field; TypeFields) {
        if (field in bo) {
            alias FieldType = typeof (mixin (`Type.` ~ field));
            FieldType toAssign;
            static if (field == `_id` && is (FieldType == string)) {
                // Slightly modified version of Mondo's ObjectId.toString ()
                // Allows casting the ObjectId back to a string.
                static immutable char[] digits = "0123456789abcdef";
                auto app = appender!string;
                foreach (b; bo [field].to!ObjectId._data) {
                    app.put (digits [b >> 4]);
                    app.put (digits [b & 0xF]);
                }
                toAssign = app.data;
            } else {
                toAssign = bo [field].recursiveArrayMap! (FieldType);
            }
            enum fieldToAssign = `toReturn.` ~ field;
            mixin (fieldToAssign ~ ` = toAssign;`);
        }
    }
    return toReturn;
}

unittest {
    struct Test {
        int a = 3;
        int b = 5;
        string c = `Foo`;
        int d;
        bool e = true;
        int [] f = [6,5,4]; 
    }
    struct WithId {
        int s = 3;
        string _id = "aaaaaaaaaaaaaaaaaaaaaaaa";
    }
    struct WithObjectId {
        ObjectId _id = "cccccccccccccccccccccccc";
    }
    auto comparedTo = Test (3, 5, `Bar`);
    assert (fromBO!Test (BO (`c`, `Bar`)) == comparedTo);
    // Test that operations are the inverse of the other one.
    assert (comparedTo.bson.fromBO!Test == comparedTo);
    auto idCheck = WithId (3, `bbbbbbbbbbbbbbbbbbbbbbbb`);
    auto boWithId = BO (`_id`, ObjectId (`bbbbbbbbbbbbbbbbbbbbbbbb`));
    assert (fromBO!WithId (boWithId) == idCheck);
    auto objectIdCheck = WithObjectId (ObjectId(`bbbbbbbbbbbbbbbbbbbbbbbb`));
    assert (fromBO!WithObjectId (boWithId) == objectIdCheck);
}

import std.algorithm : map;

/// Used to handle arrays because Mondo uses BsonArrays.
auto recursiveBsonArray (Type)(Type input) {
    // One-dimensional arrays can avoid the need of a BsonArray.
    static if (isArray!Type && !is (Type == string)) {
        // Slice operator is used to allow static arrays.
        return BsonArray (input [].map!(a => a.recursiveBsonArray).array);
    } else {
        return input;
    }
}

unittest {
    assert (recursiveBsonArray (5) == 5);
    assert (recursiveBsonArray ([1,2,3]) == BA([1,2,3]));
    import std.stdio;
    assert (recursiveBsonArray ([[3,4,5], [1,2], []]) 
        == BA([BA(3,4,5), BA(1,2), BA()]));
    assert (recursiveBsonArray (`Hello`) == `Hello`);
}

auto recursiveArrayMap (Type)(BsonValue input) {
    static if (isArray!Type && !is (Type == string)) {
        return input
            .to!(BsonArray)
            .map!(a => a.recursiveArrayMap!(ElementType!Type))
            .array;
    } else {
        import std.conv : to;
        return input.to!Type;
    }
}

unittest {
    assert (recursiveArrayMap!int (BsonValue (5)) == 5);
    assert (recursiveArrayMap! (int [])(BsonValue (BA ([1,2,3]))) == [1,2,3]);
    assert (recursiveArrayMap! (string [])(BsonValue (BA ([`Foo`, `Bar`, ``])))
        == [`Foo`, `Bar`, ``]);
    assert (recursiveArrayMap! (int [][])(BsonValue (BA ([BA([1,2]), BA([3,4])])))
        == [[1,2], [3,4]]);
}
