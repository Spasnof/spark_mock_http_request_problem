# mocking urls in spark scala
Just a sample repo to think through some problems 

# helpful links:
- [how to mock core functionality](https://stackoverflow.com/questions/16443801/mockito-for-objects-in-scala)
- [difference between spy and mock](https://stackoverflow.com/questions/28295625/mockito-spy-vs-mock#:~:text=The%20difference%20is%20that%20in,call%20the%20real%20method%20behavior)
- [how to use traits and mockito](https://blog.knoldus.com/mocking-the-right-way/)


# Problem space 
At a low level wse use fromURL which is not testable in a unit test and will require a live https server. We likely want to make the fromURL an arg.
```
/* load something from a url */ 
 def loadFromUrl(url: String) : String = {
    val sourceBuffer = scala.io.Source.fromURL(url)
    try {
      sourceBuffer.mkString
    } finally {
      sourceBuffer.close()
    }
  }
```
At the next level we are appending these to data frames
```
/* load a url and append to the df */
def loadUrlOfJsonObjectAndAppend[T <: Product : TypeTag](urlColumn: Column, newColumnName: String)(df: DataFrame) : DataFrame = {
    val urlToJsonUDF = udf[String, String](UserDefinedFunctions.loadFromUrl)

    df.withColumn(
      newColumnName,
      from_json(urlToJsonUDF(urlColumn), Encoders.product[T].schema)
    )
  }
```
This is how we are loading from json
```
/* spark sql from_json *
org.apache.spark.sql.functions.from_json(column, encoding)
```


# Problems created by solution:
Note this is all pseudo code.
```
// low level function to external resource, give it a default so we can mock it
def call_database(query, db_api_call=standard_connection):
	returns db_api_call(query)

// usage of the low level function this is what we want to test.
def add_one(db_api_call=standard_connection) // <-- THIS IS NON IDEAL AND NEEDS TO BE DONE ON EVERY LEVEL TO TEST
	return call_database(db_api_call) + 1

// higher level usage that still uses the low level call, this is not tested
def job()
	// do stuff before
	val blah = add_one()
	// do stuff after



// test of the low level call
def test_add_one()
	def mock_db_api_call(query)
		return 1
	val expected = 2
	val output = add_one(mock_db_api_call)
	assert(expected, output)

// problem area I don't want to pass the context of what this db_api_call is just for the sake of testing. Ideally the call_database() is the only thing we replace at runtime just for the tests.

```