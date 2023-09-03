# Batch_Analytics_on_Bike_Share_Problem_Using_Hadoop
Simulate Hadoop to solve Big Data Analytic problems (Bike Share Problem). 


**Data Analytics Design**

The program depends on BikeShareMapper class and BikeShareReducer class, which are a subclass of Mapper and Reducer class respectively. In this program, the user can choose to show the list of seasons or the list of stations. However, previously, we convert the file into CSV format, so that the map function in BikeShareMapper class can split and filter the text into lists of token of words.

In BikeShareMapper, we override the ‘map’ method, by utilizing three other methods: seasonParser (to return season string), stationParser (to return station string), and isTableHeader (to check whether a string is a table header). The output of the map function consists of a collection of key-value pairs (e.g., key parsedWords with each value 1), which become the input of the reduce method in BikeShareReducer class.

The reduce() method takes the collection of key-value pairs ([parsedWords,1]) and sum it up into another collection of key-value pairs ([parsedWords,numOfWords]). This collection finally becomes the result. Originally, inside of the Base ‘Reducer’ class of Hadoop, there is shuffle process, in which the key-value pairs ([parsedWords,1]) being grouped, and the sort and aggregate process which sort the key-value pairs ([parsedWords,1]) according to alphabet, and then sum up (aggregate) the value, according to the key in order to obtain key-value pairs ([parsedWords,numOfWords]).



**Results**

**Number of Station and Season (alphabetical order)**

|**Station**|**count**||**Season**|**count**|
| :- | :- | :- | :- | :- |
|abraham|48||fall|329|
|alexandria|48||spring|217|
|arlington|61||summer|235|
|capitollhill|96||winter|218|
|chinatown|96||||
|church|47||||
|churchst|60||||
|columbuscircle|60||||
|fairfax|62||||
|jefferson|64||||
|lincoln|37||||
|logan|43||||
|massachusett|64||||
|oxford|48||||
|southwest|43||||
|unionroad|62||||
|washintonmonument|60||||


**Most popular rental season:** fall (329)

**Most popular rental station:** capitolhill and Chinatown (96)

<br/>



**Discussion of Results**

From the result obtained, we can see that capitolhill and chinatown is the most popular rental station where people use this BikeShare service (96 counts during the year 2011 until 2013). For three years (2011-2013) during the fall, people use the BikeShare service most (329 usages). On the other hand, the least popular season is spring (only 217), and the least popular station is Lincoln (only 37) during 2011-2013.



**the shell commands :**

rm -r output

hadoop com.sun.tools.javac.Main BikeShare.java 

jar cf BikeShare.jar BikeShare\*.class

hadoop jar BikeShare.jar BikeShare input output

