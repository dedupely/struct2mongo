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

// A BsonObject converted to BO it's just itself.
auto bson (BO b) { return b;}

auto bson (Type)(Type instance) {
    // If an empty BO constructor is used, it segfaults when appending.
    // Already fixed in master, still not pushed to dub.
    auto toReturn = BO (`a`, `b`);
    toReturn.remove (`a`);
    static foreach (field; FieldNameTuple!Type) { {
        auto instanceField = __traits (getMember, instance, field);
        // Compare the fields of each one.
        if (instanceField != __traits(getMember, Type.init, field)
        ) {

            auto toInsert = recursiveBsonArray (instanceField);
            toReturn.append (field, toInsert);
        }
    } }
    return toReturn;
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
            enum fieldToAssign = `toReturn.` ~ field;
            auto toAssign = bo [field]
                .recursiveArrayMap!(FieldType);
            mixin (fieldToAssign ~ ` = toAssign;`);
        }
    }
    return toReturn;
}

import std.algorithm : map;
/// Used to handle arrays because Mondo uses BsonArrays.

auto recursiveBsonArray (Type)(Type input) {
    // One-dimensional arrays can avoid the need of a BsonArray.
    static if (isArray!Type && !is (Type == string)) {
        return BsonArray (input.map!(a => a.recursiveBsonArray).array);
    } else {
        return input;
    }
}

auto recursiveArrayMap (Type, R)(R input) {
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
