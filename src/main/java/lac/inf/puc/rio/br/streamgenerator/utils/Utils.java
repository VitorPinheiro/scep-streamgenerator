/**
 * 
 */
package lac.inf.puc.rio.br.streamgenerator.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import eu.larkc.csparql.cep.api.RdfQuadruple;
import lac.inf.puc.rio.br.streamgenerator.model.StreamSnipBlocks;
import lac.inf.puc.rio.br.streamgenerator.model.TriplesBlock;

/**
 * @author vitor
 *
 */
public class Utils 
{
	
	/**
	 *  Get a json with the format of blocks of triples and convert to to an ArrayList of TriplesBlock
	 * @param jsonSnip
	 * @return
	 */
	public static ArrayList<TriplesBlock> getTriplesBlockFromJSONSnip(String jsonSnip)
	{
		String tripleList = getTriplesJsonFromAtributteMsg(jsonSnip);	    
	    JsonArray arrayTriples = parseJsonArrayIntoString(tripleList);
	    
	    ArrayList<TriplesBlock> triples = new ArrayList<TriplesBlock>();
	    
	    //System.out.println("arrayTriples.size() = "+arrayTriples.size());
	    for(int i=0; i< arrayTriples.size(); i++)
	    {
	    	JsonArray blockTriplesArray = parseJsonArrayIntoString(arrayTriples.get(i).toString());
	    	//System.out.println("blockTriplesArray.size() = "+blockTriplesArray.size());
	    	String tripleJson = blockTriplesArray.get(0).toString();
	    	
	    	JsonArray blockTriplesArray2 = parseJsonArrayIntoString(tripleJson);
	    	
	    	TriplesBlock block = new TriplesBlock();
	    	//System.out.println("blockTriplesArray2.size() = "+blockTriplesArray2.size());
	    	for(int j=0; j< blockTriplesArray2.size(); j++)
	    	{
	    		
	    		String tripleJson2 = blockTriplesArray2.get(j).toString();
	    		
	    		
	    		//System.out.println("tripleJson = "+tripleJson2);
	    		RdfQuadruple triple = new RdfQuadruple(getAttributeFromMsg(tripleJson2,  "Subject"), 
						getAttributeFromMsg(tripleJson2,  "Predicate"), 
					 	getAttributeFromMsg(tripleJson2,  "Object"), 
						Long.valueOf(getAttributeFromMsg(tripleJson2,  "Timestamp")));	    		
	    		
	    		
	    		block.addTriple(triple);
	    	}
	    	
	    	triples.add(block);
	    }
	    return triples;
	}
	
	/**
     * Returns substring of attribute given, used for getting Json Array
     * @param jsonMsgStr A mensagem string no formato json.
     * @param attribute O atributo desejado da mensagem.
     * @return
     */
    public static String getTriplesJsonFromAtributteMsg(String jsonMsgStr)
    {
		JsonObject jsonMsg; 
		String ret = null;
		JsonParser parser = new JsonParser();
		try {
		    jsonMsg = parser.parse(jsonMsgStr).getAsJsonObject();
		    ret = jsonMsg.get("Triples").toString();
		}
		catch (Exception e){
		    //e.printStackTrace();
		    System.out.println("Invalid json format e2"+ "Triples");
		    return null;
		}
		return ret;
    }
	
	public static String getAttributeFromMsg(String jsonMsgStr, String attribute)
    {
		JsonObject jsonMsg; 
		String ret = null;
		JsonParser parser = new JsonParser();
		try {
		    jsonMsg = parser.parse(jsonMsgStr).getAsJsonObject(); //new Gson().fromJson(jsonMsgStr, JsonObject.class);
	
		    if(jsonMsg.get(attribute) == null)
		    {
		    	System.out.println("There is no such attriute: "+attribute);
		    	return null;
		    }
	
		    ret = jsonMsg.get(attribute).toString().replaceAll("\"" , "");
		}
		catch (Exception e){
		    //e.printStackTrace();
		    System.out.println("Invalid json format: "+ attribute);
		    return null;
		}
	
		//System.out.println("Valid json format");
		return ret;
    }
	
	/**
     * Returns String of element of Json array of position index
     * @param arraySensorDataObjectsJson
     * @param index
     * @return element of jsonArray as string
     * @author Pumar
     */
    public static JsonArray parseJsonArrayIntoString(String arraySensorDataObjectsJson)
    {
		JsonParser parser = new JsonParser();
	
		JsonArray objectsArray = (JsonArray) parser.parse(arraySensorDataObjectsJson);
	
		return objectsArray;
    }
	
	public JsonObject streamBlocksToJsonObject(StreamSnipBlocks triplesBlock, String nodeID, String msgNumber, String producer, boolean isRDFgraph)
	{
		if(triplesBlock == null)
		{
			error("triples == null. Must have triples to send.");
			return null;
		}
		
		if(nodeID == null)
		{
			error("nodeID = null. Must have a nodeID");
			return null;
		}
		
		if(msgNumber == null)
		{
			error("msgNumber = null. Must have a msgNumber.");
			return null;
		}
		
		if(producer == null)
		{
			error("producer = null. Must have a producer.");
			return null;
		}
		
		JsonObject jsonObj = new JsonObject();

		jsonObj.addProperty("ID", nodeID+"_"+producer+"_"+msgNumber); // O ID da snip gerada.
		jsonObj.addProperty("Producer", producer.toString()); // O ID da query que gerou essa snip ou o ID da stream inicial
		
		if(isRDFgraph)
			jsonObj.addProperty("isRDFgraph", "true");
		else
			jsonObj.addProperty("isRDFgraph", "false");		
		
		jsonObj.add("Triples", blockToJsonArray(triplesBlock));
		
		return jsonObj;
	}
	
	/**
     * 
     * @param triples The triples of the snip.
     * @param nodeID The ID of the node which this snip was produced.
     * @param msgNumber It is only a counter that says how many messages the query on the nodeID generated.
     * @param producer The query that produced this snip.
     * @return
     */
	public JsonObject toJsonObject(List<RdfQuadruple> triples, String nodeID, String msgNumber, String producer, boolean isRDFgraph)
	{
		if(triples == null)
		{
			error("triples == null. Must have triples to send.");
			return null;
		}
		
		if(nodeID == null)
		{
			error("nodeID = null. Must have a nodeID");
			return null;
		}
		
		if(msgNumber == null)
		{
			error("msgNumber = null. Must have a msgNumber.");
			return null;
		}
		
		if(producer == null)
		{
			error("producer = null. Must have a producer.");
			return null;
		}
		
		JsonObject jsonObj = new JsonObject();

		jsonObj.addProperty("ID", nodeID+"_"+producer+"_"+msgNumber); // O ID da snip gerada.
		jsonObj.addProperty("Producer", producer.toString()); // O ID da query que gerou essa snip ou o ID da stream inicial
		
		if(isRDFgraph)
			jsonObj.addProperty("isRDFgraph", "true");
		else
			jsonObj.addProperty("isRDFgraph", "false");
		
		jsonObj.add("Triples", toJsonArray(triples));
		
		return jsonObj;
	}
	
	public JsonArray blockToJsonArray(StreamSnipBlocks triplesBlock)
	{
		JsonArray array = new JsonArray();
		try 
		{		
			//System.out.println("triplesBlock.getNumberOfBlocks() = "+triplesBlock.getNumberOfBlocks());
			for(int j=0;j<triplesBlock.getNumberOfBlocks();j++)
			{				
			    TriplesBlock block = triplesBlock.getBlockWithOutDeleting(j); 
			    
		    	//JsonObject jsonObj = new JsonObject();
			    JsonArray innerArray = new JsonArray();
		    	
		    	//jsonObj.add("Block"+j, toJsonArray(block.getTriples()));
			    innerArray.add(toJsonArray(block.getTriples()));
		    	array.add(innerArray);
			}
		    
		} catch (Exception e) {
		    error(e.getMessage());
		    return null;
		}
		
		return array;
	}
	
	public JsonArray toJsonArray(List<RdfQuadruple> triples) 
	{
		try 
		{
		    JsonArray array = new JsonArray();
		    		    
		    for(int i=0; i<triples.size();i++) 
		    {
		    	JsonObject json_object = new JsonObject();
				json_object.addProperty("Subject", triples.get(i).getSubject());
				json_object.addProperty("Predicate", triples.get(i).getPredicate());
				json_object.addProperty("Object", triples.get(i).getObject());
				json_object.addProperty("Timestamp", triples.get(i).getTimestamp());
				
				array.add(json_object);
		    }
		    
		    return array;
		} catch (Exception e) {
		    error(e.getMessage());
		    return null;
		}
	}
	
	public JsonArray toJsonArray(String[] list)
	{
		try 
		{
		    JsonArray array = new JsonArray();
		    		    
		    for(int i=0; i<list.length;i++) 
		    {
		    	JsonObject json_object = new JsonObject();
				json_object.addProperty("Gateway", list[i]);				
				array.add(json_object);
		    }
		    
		    return array;
		} catch (Exception e) {
		    error(e.getMessage());
		    return null;
		}
	}
	
	public static BufferedReader readFileAsStream(String path) 
	{
        try {
          if (path == "-") {
            return new BufferedReader(
              new InputStreamReader(System.in));
          }
          return new BufferedReader(
            new FileReader(path));
        }
        catch (FileNotFoundException e) 
        {
        	error(String.format("cannot access '%s'", new Object[] { path }));
          
          throw new AssertionError(); 
        }
	}
	
	public static ArrayList<RdfQuadruple> readTriples(BufferedReader input)
    {
    	ArrayList<RdfQuadruple> triples = new ArrayList<RdfQuadruple>();
    	try
        {
          String line;
          while ((line = input.readLine()) != null) 
          { 
            String[] toks = line.split(" +");
            if (toks.length != 4) {
            	error("bad line: " + line);
            }
            
            long time = parseTime(toks[3]);
            if (time < 0L) {
              error("bad time value: " + toks[3]);
            }
            
            
            RdfQuadruple q = new RdfQuadruple(toks[0], toks[1], toks[2], parseTime(toks[3]));
            
            /*RdfQuadruple q = new RdfQuadruple(toks[0], toks[1], toks[2], System.currentTimeMillis());
            try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
            
            //System.out.println("Line added: "+q+" at "+q.getTimestamp() / 1000 + "secs");
            
            triples.add(q);
          }      
          
        } catch (Exception e) {
        	error("read error: " + e.getMessage());
        } 
    	
    	return triples;
    }
	
	private static long parseTime(String str) 
	{
        String[] toks = str.split(":");
        if (toks.length == 1)
          return Long.parseLong(toks[0]);
        if (toks.length == 3) {
          long h = Long.parseLong(toks[0]);
          long m = Long.parseLong(toks[1]);
          long s = Long.parseLong(toks[2]);
          h = h > 0L ? h : 0L;
          m = m > 0L ? m : 0L;
          s = s > 0L ? s : 0L;
          return (h * 3600L + m * 60L + s * 1L) * 1000L;
        }
        return -1L;
	}	
	
	public static void error(String msg) 
	{
        System.err.println("error: " + msg);
        System.exit(1);
    }
}
