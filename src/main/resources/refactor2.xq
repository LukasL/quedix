declare variable $subcollection external;
declare variable $collectionname external;

for $doci in collection($subcollection) 
return for $d in $doci/subcollection/document
return db:add(doc($collectionname),document{$d/child::node()},string($d/@path))
