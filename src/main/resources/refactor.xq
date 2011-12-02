declare variable $subcollection external;
declare variable $collectionname external;

for $d in doc($subcollection)/subcollection/document
return db:add(doc($collectionname),document{$d/child::node()},string($d/@path))
