module struct2mongo;

import mondo;
import bsond;
import std.traits, std.range;

enum MongoKeep;
enum MongoUpdated;
enum MongoCreated;
struct Col {
    Collection collection;

    auto trySeveralTimes (alias Op, uint timesT = 10, Args...)(Args args) {
        uint times = timesT;
        tryAgain:
        try {
            return Op (args);
        } catch (MongoException e) {
            if (e.domain == ErrorDomains.SERVER_SELECTION
                || e.domain == ErrorDomains.STREAM) {

                import std.stdio;
                writeln (`Connection failed, trying again.`);
                import core.thread;
                Thread.sleep (1.dur!`seconds`);
                times --;
                if (times == 0) {
                    throw e;
                } else {
                    goto tryAgain; // Yeah.
                }
            }
        }
    }

    unittest {
        import std.stdio;
        struct TestStruct {
            int foo;
        }

        Mongo mongo = new Mongo("mongodb://localhost");
        assert (mongo.connected, `Not connected to Mongo`);
        auto collection = Col (mongo [`newBase`][`newCollection`]);
        // Be careful, this deletes the Collection.
        if (collection.exists) collection.drop;

        return collection.trySeveralTimes!insert(TestStruct (3));
    }

    // Version without ref for non-lvalues.
    auto insert (S)(S val) { this.insert (val); }
    auto insert (S)(ref S val) {
        alias created = getSymbolsByUDA!(S, MongoCreated);
        static assert (
            created.length < 2
            , `Are you sure you want several MongoCreated`
            ~ ` symbols in ` ~ S.stringof ~ `?`
        );
        import std.datetime;
        static if (created.length) {
            static assert (is (typeof (created [0]) == long));
            mixin (
                `val.` ~ __traits (identifier, created [0])
                ~ ` = Clock.currTime (UTC ()).toUnixTime;`
            );
        }
        collection.insert (val.bson);
    }

    // Version without ref for non-lvalues.
    void update (S)(
          in BsonObject selector
        , S update
        , in UpdateFlags flags = UpdateFlags.NONE
        , in WriteConcern writeConcern = null
    ) {
        this.update!S (selector, update, flags, writeConcern);
    }

    void update (S)(
          in BsonObject selector
        , ref S update
        , in UpdateFlags flags = UpdateFlags.NONE
        , in WriteConcern writeConcern = null
    ) {
        static if (is (S == BO)) {
            // Mondo's
            collection.update (selector, update, flags, writeConcern);
        } else {
            alias updated = getSymbolsByUDA!(S, MongoUpdated);
            static assert (
                updated.length < 2
                , `Are you sure you want several MongoUpdated`
                ~ ` symbols in ` ~ S.stringof ~ `?`
            );
            import std.datetime;
            static if (updated.length) {
                static assert (is (typeof (updated [0]) == long));
                mixin (
                    `update.` ~ __traits (identifier, updated [0]) 
                    ~ ` = Clock.currTime (UTC ()).toUnixTime;`
                );
            }
            collection.update (selector, update.bson, flags, writeConcern);
        }
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

    struct UpdatedTest {
        int val = 0;
        @MongoUpdated long updateTime  = 0;
        @MongoCreated long createdTime = 0;
    }
    auto ut = UpdatedTest (1);
    assert (ut.createdTime == 0);
    collection.insert (ut);
    assert (ut.createdTime != 0);
    ut.val = 2;
    assert (ut.updateTime == 0);
    collection.update (BO (`val`, 1), ut);
    assert (ut.updateTime != 0);

    collection.remove (BO (`val`, 2));

    // Test Mondo's methods.
    assert (
        collection
        .aggregate (BA ([BO(`$match`, BO (`a`, 8))]))
        .front [`a`] == 8
    );
    assert (collection.find.array.length == 2);
    assert ((`a` !in collection.findOne ()) || collection.findOne ()[`a`] == 8);

    auto find16 = new Query;
    find16.conditions = BO (`a`, 16);
    collection.update (BO (`a`, 8), BO (`a`, 16));
    assert (! collection.find (find16).empty);

}

// A BsonObject converted to BO it's just itself.
auto bson (BO b) { return b;}

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

/// Converts bo to Type by using the field names of Type and keys of bo.
auto fromBO (Type) (BO bo) {
    static assert (__traits (isPOD, Type)
        , "fromBO is made for POD structs.\n"
        ~ ` Make sure it's okay to assign to ` ~ Type.stringof
        ~ `'s fields and comment this warning.`);
    alias TypeFields = FieldNameTuple!Type;
    Type toReturn;
    foreach (key, val; bo) {
        outerSwitch: switch (key) {
            static foreach (field; TypeFields) {
                case field:
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
                    break outerSwitch;
            }
            default:
                // _id is the only field that is allowed to be on the BO
                // and not on the struct, if bo has some other field that the
                // struct doesn't, an exception is thrown.
                if (key != `_id`)
                    throw new Exception (`Found member of BO that is not in `
                        ~ Type.stringof ~ ` : ` ~ key);
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

    // Using a BO with other fields should throw an exception:
    auto extraFields = BO (`a`, 3, `g`, 8);
    import std.exception;
    assertThrown (fromBO!Test (extraFields));
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
