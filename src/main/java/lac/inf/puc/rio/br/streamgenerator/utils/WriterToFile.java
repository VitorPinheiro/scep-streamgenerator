/**
 * 
 */
package lac.inf.puc.rio.br.streamgenerator.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * @author vitor
 *
 */
public class WriterToFile 
{
	private String _filePath;	
	private String _streamGenFile = "StreamGen.txt";
	private String _tweetsFile = "Tweets.txt";
	
	
	public WriterToFile(String callerName, Boolean deleteTweetsFile)
	{
		_filePath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
		initialize(deleteTweetsFile);
	}
	
	private void initialize(Boolean deleteTweetsFile)
	{
		// Delete files
		File file = new File(_filePath+_streamGenFile);
		File file2 = new File(_filePath+_tweetsFile);

		if(file.delete())
        { 
            System.out.println(_streamGenFile+" deleted successfully"); 
        } 
        else
        { 
            System.out.println("Failed to delete the file: "+_filePath+_streamGenFile); 
        }

        if(deleteTweetsFile)
		if(file2.delete())
		{
			System.out.println(_tweetsFile+" deleted successfully");
		}
		else
		{
			System.out.println("Failed to delete the file: "+_filePath+_tweetsFile);
		}
	}
	
	
	public void writeToStreamGeneratorFileNewLine(String time, int executionNum, int numTriplesOut)
	{
		System.out.println("Writing to this file = "+_filePath+_streamGenFile);
		PrintWriter out = null;
		try
		{
			out = new PrintWriter(new BufferedWriter(new FileWriter(_filePath+_streamGenFile, true)));
		    out.println("Execution "+executionNum+": "+time+ " "+numTriplesOut);
		} 
		catch (IOException e) 
		{
			 System.err.println(e);
		}
		finally 
		{
			if (out != null) {
		        out.close();
		    }
		}
	}
	
	/**
	 * Each json msg has the size delimited on the TweetStreamGenerator.
	 * @param fromMsg
	 * @param numberOfMsgs
	 * @return
	 */
	public List<String> readJsonMsgsFromFile(int fromMsg, int numberOfMsgs)
	{
		// We need to provide file path as the parameter: 
		  // double backquote is to avoid compiler interpret words 
		  // like \test as \t (ie. as a escape sequence) 
		  int msgsRead = 0;
		  int lastIndexToRead = numberOfMsgs + fromMsg;		  
		
		  File file = new File(_filePath+_tweetsFile); 
		  
		  List<String> ret = new ArrayList<String>();
		  
		  BufferedReader br;
		  try {
			br = new BufferedReader(new FileReader(file));
		 		  
		  String st; 
		  while ((st = br.readLine()) != null) 
		  {
		    
			if(msgsRead >= fromMsg)
			{
				//System.out.println(st);
				ret.add(st);
			}
			
		    msgsRead++;
		    
		    if(msgsRead >= lastIndexToRead)
		    	break;
		  }
		  
		  } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		  }
		  
		  System.out.println("(WriterToFile.readJsonMsgsFromFile) Numero de json msgs = "+ret.size());
		  if(ret.size() == 0)
			  return null;
		  
		return ret;
	}
	
	/**
	 * Cada linha deste arquivo é uma mensagem json inteira contendo numTweetsPerTime tweets.
	 * @param jsonAsString
	 */
	public void persistJsonMsgs(String jsonAsString)
	{
		System.out.println("Writing tweets to this file = "+_filePath+_tweetsFile);
		PrintWriter out = null;
		try
		{
			out = new PrintWriter(new BufferedWriter(new FileWriter(_filePath+_tweetsFile, true)));
		    out.println(jsonAsString);
		} 
		catch (IOException e) 
		{
			 System.err.println(e);
		}
		finally 
		{
			if (out != null) {
		        out.close();
		    }
		}
	}
}