import java.io.IOException;
import java.util.Scanner; 
import java.util.regex.*; //use regex package to simplify the mapper function

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
[1] isTableHeader method
[2] seasonParse method
[3] stationParser method
[4] BikeShareMapper 
[5] BikeShareReducer
*/

public class BikeShare {

  static String choice; //decide whether you want to show 'season' or 'station'.

/*
[1] isTableHeader method: takes string input 'checkedWord', and checks whether it matches the list of the table header(s). 
	If yes, it returns a 'true' value.
	For differentiating whether it is the header or the cell values.
	*/
  static boolean isTableHeader(String checkedWord){
   checkedWord = checkedWord.trim();
	Pattern pattern = Pattern.compile("dteday|seasons|yr|mnth|weekday|weathersit|casual|stations"); //lists all table headers
	Matcher matcher = pattern.matcher(checkedWord);
		if (matcher.find()) //if match with the list above, return true.
			{return true;}
		else
			{return false;}
	}
  
  
/*
[2] seasonParse method: takes string input 'seasonName', and checks whether it matches the list of the seasons. 
	If yes, it returns the match string.
	*/
  static String seasonParser(String seasonName){
   String parsedSeason = "";
   seasonName = seasonName.trim();
	Pattern pattern = Pattern.compile("spring|summer|fall|winter"); //lists all seasons.
	Matcher matcher = pattern.matcher(seasonName);
		if (matcher.find())  //if match with the list above, then it is a 'season'.
			{parsedSeason = seasonName;}
	return parsedSeason;
	}
  

/* 
[3] stationParser method: takes string input 'stationName', and checks whether it matches the criteria or station name, 
	(based on regex filter, since the csv file also contains numbers in other columns). 
	If yes, it returns the match string.
	*/
static String stationParser(String stationName){
   	String parsedStation = "";
	stationName = stationName.trim();
	Pattern pattern = Pattern.compile("[a-z]+"); //define regex pattern for alphabet, to filter the csv file.
	Matcher matcher = pattern.matcher(stationName);
		if (matcher.find() && seasonParser(stationName).isEmpty() && !isTableHeader(stationName)) //If it is NOT a season, and NOT a table header, then it is a 'station'. The reason is that other data in the CSV are numerical data.
			{parsedStation = stationName;}
	return parsedStation;
	}

/*
[4] BikeShareMapper : Split the text in file and return list of selected words with value 1
*/	
  public static class BikeShareMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
		String[] words = value.toString().split(","); //split the data in csv by comma (',')
		String parsedWords="";
		for (int i=0; i<words.length; i++){
			if (choice.equals("1") && seasonParser(words[i])!=null && !seasonParser(words[i]).isEmpty()){ //if user choose 1 : it will show the list of the seasons in the output folder.
				parsedWords = seasonParser(words[i]);
			}else if (choice.equals("2") && stationParser(words[i])!=null && !stationParser(words[i]).isEmpty()){ //if user choose 2 : it will show the list of the stations in the output folder.
				parsedWords = stationParser(words[i]);
			}else{
				continue; //skip if it isn't related to season or station.
			}     		
			word.set(parsedWords);
			context.write(word, one);
		}
    }
}

/*
[5] BikeShareReducer : Shuffling, sorting and aggregating (summing up) the words (key), so we get numOfWords as its value.
*/	
  public static class BikeShareReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
		int numOfWords = 0;
		for (IntWritable val : values) {
			numOfWords += val.get();
		}
	result.set(numOfWords);
	context.write(key, result);
    }
}

  public static void main(String[] args) throws Exception {

    Scanner question = new Scanner(System.in);  // Create a Scanner object for interacting with the user choice
	do{
		System.out.println("Please choose : \n 1. Show how many number of season(s). \n 2. Show how many number of terminal(s). \n x. Exit program. \n Your choice (1 or 2)? : ");
		choice = question.nextLine();  // Read user input
		System.out.println("Your choice is: " + choice);  // Output user input
	}while(!(choice.equals("1") || choice.equals("2") || choice.equals("x")));
		
	if (choice.equals("x")) {
		System.out.println("Program terminated safely....Thank you.");
		System.exit(0);
	}
	
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "bike share syahirul");
    job.setJarByClass(BikeShare.class);
    job.setMapperClass(BikeShareMapper.class);
    job.setCombinerClass(BikeShareReducer.class);
    job.setReducerClass(BikeShareReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    
  }
}
