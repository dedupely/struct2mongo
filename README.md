# struct2mongo
A Dlang Mongo wrapper of Mondo that allows plug &amp; play usage of structs. Uses the Mondo library to perform its operations.

## Example usage:
```d
void main () {
  import mondo;
  import struct2mondo;
  Mongo mongo = new Mongo("mongodb://localhost");
  assert (mongo.connected, `Not connected to Mongo`);
  // Col is the name of struct2mongo's Collection to avoid name clashes.
  auto collection = Col (mongo [`newBase`][`newCollection`]);
  // Be careful if copy pasting this.
  if (collection.exists) collection.drop();

  // true isn't saved because it's the default value.
  auto contact = Contact (`(800)34514129`, true, [3,4,5]);
  collection.insert (contact);
  writeln ('\n', `After contact insertion: `, '\n');
  writeln (collection.findOne!Contact());
}

struct Contact {
    string value;
    bool active = true;
    int [] favoriteNumbers = [1, 2, 3];
}
```
