ant runtest -Dtest=TupleTest &&
ant runtest -Dtest=TupleDescTest &&
ant runtest -Dtest=CatalogTest &&
ant runtest -Dtest=HeapPageIdTest &&
ant runtest -Dtest=RecordIdTest &&
ant runtest -Dtest=HeapPageReadTest &&
ant runtest -Dtest=HeapFileReadTest &&
ant runtest -Dtest=PredicateTest &&
ant runtest -Dtest=JoinPredicateTest &&
ant runtest -Dtest=FilterTest &&
ant runtest -Dtest=JoinTest &&
ant runtest -Dtest=IntegerAggregatorTest &&
ant runtest -Dtest=StringAggregatorTest &&
ant runtest -Dtest=AggregateTest &&
ant runtest -Dtest=HeapPageWriteTest &&
ant runtest -Dtest=HeapFileWriteTest &&
ant runtest -Dtest=BufferPoolWriteTest &&
ant runtest -Dtest=InsertTest &&

ant runsystest -Dtest=ScanTest &&
ant runsystest -Dtest=FilterTest &&
ant runsystest -Dtest=JoinTest &&
ant runsystest -Dtest=AggregateTest &&
ant runsystest -Dtest=InsertTest &&
ant runsystest -Dtest=DeleteTest