/**
 * 
 */
package lac.inf.puc.rio.br.streamgenerator;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.gson.JsonObject;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

import eu.larkc.csparql.cep.api.RdfQuadruple;
import eu.larkc.csparql.core.ResultFormatter;
import lac.inf.puc.rio.br.streamgenerator.model.StreamSnipBlocks;
import lac.inf.puc.rio.br.streamgenerator.model.TriplesBlock;
import lac.inf.puc.rio.br.streamgenerator.utils.IKafkaConstants;
import lac.inf.puc.rio.br.streamgenerator.utils.Utils;
import lac.inf.puc.rio.br.streamgenerator.utils.WriterToFile;

/**
 * @author vitor
 *
 *	ESSA CLASSE TEM QUE SEMPRE MANDAR UM NUMERO MENOR DE TRIPLAS DO QUE O TAMANHO DA JANELA!
 * 	ISSO VAI GARANTIR QUE NENHUM TWEET TENHA SUAS TRIPLAS DIVIDIAS EM JANELAS DIFERENTES.
 * 	VAI GERAR OVERHEAD EM TRIPLAS, MAS ISSO PODE SER CONTADO.
 *	
 */
public class TweetStreamGenerator {

	
	private static Utils _utils;
	private static UUID _myUUID;
	private static Integer _msgCount;
	private static long _startTime;
	
	private static long _lastSendTime;
	private static long _waitTime;
	
	private static WriterToFile _writerToFile;
	
	private static void Initialize(Boolean deleteTweetsFile)
	{
		_utils = new Utils();
		_myUUID = UUID.fromString("bb103877-8335-444a-be5f-db8d916f6754");
		_msgCount = 0;
		_writerToFile = new WriterToFile("TweetStreamGenerator", deleteTweetsFile);
		_lastSendTime = 0l;
	}
	
	
	/**
	 * Send triples from TweetStream file using kafka.
	 * @param args
	 */
	public static void main(String[] args)
	{
		boolean isThereMoreTweets = true;
		int numMsgsPerTime = 1; // Numero de mensagens que vao ser enviadas do arquivo Tweets.txt, gerado pela outra main. Essas mensagens contém mais de um Tweet.
		// São 200 tweets por mensagem. a variavel que guarda isso é: numTweetsPerTime (esta na outra main)
		int totalAnalizedTweets = 0;
		int offset = 0;
		
		Initialize(false); // nao deleta o arquivo de Tweets - Vc precisa enviar ele
		
		while(isThereMoreTweets)
		{			
			System.out.println("From = "+offset+" NumOfMsgs = "+numMsgsPerTime);
			isThereMoreTweets = runProducer(offset, numMsgsPerTime);

			/*
			if(totalAnalizedTweets > 5)
			{
				isThereMoreTweets = false;
				break;
			}*/
			
			totalAnalizedTweets = totalAnalizedTweets + numMsgsPerTime;
			offset = offset + numMsgsPerTime;
		}
		
		if(!isThereMoreTweets)
			System.out.println("All msgs sent!");
	}
	
	/**
	 * Just generate TweetStream file. 
	 * This is the pre processing step, to transform the TweetKB data into a JSON stream for my infrastructure.
	 *
	 * Quando gerar o arquivo Tweets.txt, o topico "Producer", que indica quem produziu as mensagens vai
	 * ficar salvo no arquivo. O producer é pego no arquivo IKafkaConstants e é a variavel TOPIC.
	 * Entao quando gerar o arquivo já colocar o Producer correto nele.
	 * 
	 * @param args
	 */
	public static void main2(String[] args)
	{
		//Initialize(true); // apaga tweets
		Initialize(false); // nao apaga tweets, acumula
		Model model = ModelFactory.createDefaultModel();
				
		System.out.println("Reading model into memory...");
		model.read("examples/month_2013-01.n3");
		//model.read("examples/month_2013-02.n3");
		System.out.println("Model loaded.");
		
		System.out.println("Number of statements: "+model.size());
		
		/*StmtIterator it =  model.listStatements();
		int count = 0;
		while (it.hasNext()) {
		     Statement stmt = it.next();
		     // do your stuff with the Statement (which is a triple)
		     
		     System.out.println(stmt);
		     
		     count++;
		     if(count == 10)
		    	 break;
		}*/
		
		boolean isThereMoreTweets = true;
		//int numTweetsPerTime = 200;
		int numTweetsPerTime = 200;
		int totalAnalizedTweets = 0;
		while(isThereMoreTweets)
		{			
			_startTime = System.currentTimeMillis();
			
			isThereMoreTweets = readAndSendTweets(model, numTweetsPerTime, totalAnalizedTweets); // 12 tweets
			totalAnalizedTweets = totalAnalizedTweets + numTweetsPerTime;
			if(totalAnalizedTweets > 1000)
				break;
		}
		
		
		
		//readAndSendTweets(model, 12, 0); // 10 tweets
		//readAndSendTweets(model, 12, 12); // 10 tweets
		//readAndSendTweets(model, 12, 24); // 10 tweets
		
		//readAndSendTweets(model, 12, 36); // 10 tweets
		//readAndSendTweets(model, 12, 48); // 10 tweets
		//readAndSendTweets(model, 12, 60); // 10 tweets
		//readAndSendTweets(model, 12, 72); // 10 tweets
		//readAndSendTweets(model, 12, 84); // 10 tweets
		
		// Para cada Post do 100 achados, pegar todas as triplas relacionadas a eles e colocar o timestamp igual em todas elas.
		// Enviar os 100 posts com timestamps
		// Repetir para os proximos 100 até acabar os tweets.

		System.out.println("Arquivo de tweets gerado.");
	}
	
	private static boolean readAndSendTweets(Model model, int numberOfTweets, int offset)
	{
		///////////////////////////////////////////////////////////////////////////////////////////
		// STEP 1: Defino quais X posts eu vou enviar. (OK) : Faço isso para ordenar os posts mais 
		// antigos aos mais novos.
		///////////////////////////////////////////////////////////////////////////////////////////
		String prefixs = "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
				+ "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
				+ "prefix xsd: <http://www.w3.org/2001/XMLSchema#> "
				+ "prefix nee: <http://www.ics.forth.gr/isl/oae/core#> "
				+ "prefix schema: <http://schema.org/> "
				+ "prefix dc: <http://purl.org/dc/terms/> "
				+ "prefix sioc: <http://rdfs.org/sioc/ns#> "
				+ "prefix sioc_t: <http://rdfs.org/sioc/types#> "
				+ "prefix onyx: <http://www.gsi.dit.upm.es/ontologies/onyx/ns#> "
				+ "prefix wna: <http://www.gsi.dit.upm.es/ontologies/wnaffect/ns#> \n";
		
		String query = "select distinct ?postID ?datetime" 
										+ "{ ?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://rdfs.org/sioc/ns#Post> ."
											+ "?x <http://rdfs.org/sioc/ns#id> ?postID ."
											+ "?x sioc:has_creator ?postCreator ."
											+ "?x <http://schema.org/mentions> ?y ."
											+ "?y  <http://www.ics.forth.gr/isl/oae/core#detectedAs> ?subject ."											
											+ "?x <http://purl.org/dc/terms/created> ?datetime ."
											+ "?y  <http://www.ics.forth.gr/isl/oae/core#hasMatchedURI> ?dbpedia ."
											+ " } "
											+ "ORDER BY (?datetime)"
											+ "LIMIT "+numberOfTweets+" OFFSET "+offset;
		
		//System.out.println( "\n=== SPARQL results === numOfTweets "+numberOfTweets+" == offset "+offset + " ===" );	
		
		// Ordenado por Datatime
		ResultSet rs1 = QueryExecutionFactory.create( prefixs+query, model ).execSelect();
		
		///////////////////////////////////////////////////////////////////////////////////////////
		// STEP 2: Para cada um dos X posts eu pego todas as triplas dele crio os RDFQuadruple, 
		// usando o datetime como timestamp.
		///////////////////////////////////////////////////////////////////////////////////////////
		StreamSnipBlocks allTriples = new StreamSnipBlocks();	
		
		if(rs1.hasNext() == false)
		{ // nao tem mais nenhum resultado
			return false;
		}
		
		while(rs1.hasNext())
		{		
			QuerySolution postResult = rs1.next();
			
			//System.out.println("postResult.get(\"postID\") = "+postResult.get("postID"));
			String postID = postResult.get("postID").asLiteral().toString();
			String dateTime = postResult.get("datetime").asLiteral().toString();

			//System.out.println("=== Results for postID = "+postID+" ===");
			String query2 =    "construct { ?x schema:mentions ?entity ."
										+ " ?x sioc:id \""+postID+"\" ."
										+ " ?x rdf:type sioc:Post ."
										+ " ?x dc:created ?datetime ."
										+ " ?x sioc:has_creator ?postCreator ." 
										+ " ?entity nee:detectedAs ?subject ."
										+ " ?entity nee:confidence ?value ."
										+ " ?entity nee:hasMatchedURI ?dbpedia ."
										+ " ?x onyx:hasEmotionSet ?emoSet ."
										+ " ?emoSet onyx:hasEmotion ?positive ."
										+ " ?positive onyx:hasEmotionIntensity ?positiveNum ."
										+ " ?positive onyx:hasEmotionCategory wna:positive-emotion ."
										+ " ?emoSet onyx:hasEmotion ?negative ."
										+ " ?negative onyx:hasEmotionIntensity ?negativeNum ."
										+ " ?negative onyx:hasEmotionCategory wna:negative-emotion ."
										+ " ?x schema:interactionStatistic ?interactionLike ."
										+ " ?interactionLike rdf:type schema:InteractionCounter ."
										+ " ?interactionLike schema:interactionType schema:LikeAction ."
										+ " ?interactionLike schema:userInteractionCount ?likeCount ."
										+ " ?x schema:interactionStatistic ?interactionShare ."
										+ " ?interactionShare rdf:type schema:InteractionCounter ."
										+ " ?interactionShare schema:interactionType schema:ShareAction ."
										+ " ?interactionShare schema:userInteractionCount ?shareCount ."											
										+ " ?x schema:mentions ?TagClass ."
										+ " ?TagClass rdf:type sioc_t:Tag ."
										+ " ?TagClass rdfs:label ?tag ."
										+ " ?x schema:mentions ?UserAcc ."
										+ " ?UserAcc rdf:type sioc:UserAccount ."
										+ " ?UserAcc sioc:name ?userName . }"										
				+ "where { ?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://rdfs.org/sioc/ns#Post> ."
					+ "?x sioc:id \""+postID+"\" ."
					+ "?x schema:mentions ?entity ."
					+ "?x sioc:has_creator ?postCreator ."
					+ "?entity nee:detectedAs ?subject ."											
					+ "?x dc:created ?datetime ."
					+ "?entity nee:hasMatchedURI ?dbpedia ."
					+ "?entity nee:confidence ?value ."
					+ "?x onyx:hasEmotionSet ?emoSet ."
					+ "?emoSet onyx:hasEmotion ?positive ."
					+ "?positive onyx:hasEmotionIntensity ?positiveNum ."
					+ "?positive onyx:hasEmotionCategory wna:positive-emotion ."
					+ "?emoSet onyx:hasEmotion ?negative ."
					+ "?negative onyx:hasEmotionIntensity ?negativeNum ."
					+ "?negative onyx:hasEmotionCategory wna:negative-emotion ."
					+ "?x schema:interactionStatistic ?interactionLike ."
					+ "?x schema:interactionStatistic ?interactionShare ."
					+ "?interactionLike schema:interactionType schema:LikeAction . "
					+ "?interactionLike rdf:type schema:InteractionCounter ."
					+ "?interactionLike schema:userInteractionCount ?likeCount ."
					+ "?interactionShare schema:interactionType schema:ShareAction . "
					+ "?interactionShare rdf:type schema:InteractionCounter ."
					+ "?interactionShare schema:userInteractionCount ?shareCount ."
					+ "OPTIONAL { ?x schema:mentions ?TagClass }"
					+ "OPTIONAL { ?TagClass rdf:type sioc_t:Tag }"
					+ "OPTIONAL { ?TagClass rdfs:label  ?tag }"
					+ "OPTIONAL { ?x schema:mentions ?UserAcc }"
					+ "OPTIONAL { ?UserAcc rdf:type sioc:UserAccount }"
					+ "OPTIONAL { ?UserAcc sioc:name ?userName }"
					+ " } ";
			
			Model model2 = QueryExecutionFactory.create( prefixs+query2, model ).execConstruct();						 

			// Print into console all triples
			//outQuery("SELECT ?subject ?predicate ?object " + "WHERE { ?subject ?predicate ?object }", model2);
						
			// Parse datetime:
			dateTime = dateTime.substring(0, dateTime.indexOf("^"));									
			
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss");
			Date result = null;
			
			try 
			{
				result = df.parse(dateTime);
			} 								
			catch (ParseException e) 
			{
				_utils.error("Error to parse the date of tweet with postID = "+postID+" and with date = "+dateTime);					
				e.printStackTrace();
				return false;
			}
			
			TriplesBlock postTriplesBlock = new TriplesBlock();
			
			StmtIterator it = model2.listStatements();
			while(it.hasNext())
			{
				Statement stmt = it.nextStatement();
				
				Triple t = stmt.asTriple();
			    
				RdfQuadruple postTriple = new RdfQuadruple(t.getSubject().toString(), 
														t.getPredicate().toString(), 
														t.getObject().toString(), 
														result.toInstant().toEpochMilli());
				
				// Preciso garantir que eu envie até 248 triplas e que nenhum tweet seja dividido.
				// Esse triples deve ter no maximo 248, na hora de criar ele eu preciso monitorar isso, cada vez que der 248 eu envio pelo runProducer.
				postTriplesBlock.addTriple(postTriple);
			}
			
			//System.out.println("=== The post "+postID+" and timestamp "+result.toInstant().toEpochMilli()+" have "+postTriplesBlock.size()+" triples ===");
			
			///////////////////////////////////////////////////////////////////////////////////////////
			// STEP 3: Ensure that if adding the whole post triples it will not pass the maximum number of triples per message size.
			// If it will pass, send the current allTriples and put the post to the next batch.
			///////////////////////////////////////////////////////////////////////////////////////////
			if (allTriples.getTotalNumberOfTriples() + postTriplesBlock.size() < IKafkaConstants.NUMBER_OF_TRIPLES_PER_MSG)
			{ // OK, add it.		
				//System.out.println("Added triples "+postTriplesBlock.size()+" to allTriples");
				allTriples.addBlock(postTriplesBlock);
				//System.out.println("allTriples blocksize = "+allTriples.getNumberOfBlocks());
				//System.out.println("allTriples triple size = "+allTriples.getTotalNumberOfTriples());
			}
			else if(allTriples.getTotalNumberOfTriples() + postTriplesBlock.size() > IKafkaConstants.NUMBER_OF_TRIPLES_PER_MSG)
			{ // Send it and keep postTriples to next batch.
				System.out.println("=== Gerando um json com o tamanho ("+allTriples.getTotalNumberOfTriples()+") ===");
				//runProducer(allTriples);
				generateJsonFile(allTriples);
				allTriples.clear();
				allTriples.addBlock(postTriplesBlock);
			}
			else if(allTriples.getTotalNumberOfTriples() + postTriplesBlock.size() == IKafkaConstants.NUMBER_OF_TRIPLES_PER_MSG)
			{ // Send both.				
				allTriples.addBlock(postTriplesBlock);
				System.out.println("=== Gerando um json com o tamanho ("+allTriples.getTotalNumberOfTriples()+") ===");
				//runProducer(allTriples);
				generateJsonFile(allTriples);
				allTriples.clear();
			}
			
			postTriplesBlock.clear();
		}
		
		if(allTriples.getNumberOfBlocks() != 0)
		{
			System.out.println("=== Gerando um json com o resto das triplas que nao formaram um tamanho de mensagem ("+allTriples.getTotalNumberOfTriples()+") ===");
			//runProducer(allTriples);
			generateJsonFile(allTriples);
			allTriples.clear();
		}	
		
		return true;
	}
	
	
	private static Producer<Long, String> createProducer() 
	{
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, IKafkaConstants.CLIENT_ID);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return new KafkaProducer<Long, String>(props);
	}
	
	private static void persistJsonMsgsAsStrings(StreamSnipBlocks triples)
	{
		JsonObject jsonStream = _utils.streamBlocksToJsonObject(triples, 
				_myUUID.toString(), 
				Integer.toString(_msgCount),
				IKafkaConstants.TOPIC_NAME,
				true);
		
		// jsonStream.toString()
		
	}
	
	
	private static void generateJsonFile(StreamSnipBlocks triples)
	{
		JsonObject jsonStream = _utils.streamBlocksToJsonObject(triples, 
				_myUUID.toString(), 
				Integer.toString(_msgCount),
				IKafkaConstants.TOPIC_NAME,
				true);	

		_writerToFile.persistJsonMsgs(jsonStream.toString());
		
		_msgCount++;
		
		System.out.println("Msgs Written on file = "+_msgCount);
	}

	/**
	 * 
	 * @param offset Quantas mensagens pular das mensagens gravadas no arquivo Tweet.txt
	 * @param numMsgs Quantas mensagens enviar a partir do offset, todas do arquivo Tweet.txt
	 * @return
	 */
	private static boolean runProducer(int offset, int numMsgs) 
	{
		Long timeToBuildJsonMsg;
		
		Producer<Long, String> producer = createProducer();			
		//JsonObject jsonStream = _utils.streamBlocksToJsonObject(triples, 
			//										_myUUID.toString(), 
				//									Integer.toString(_msgCount),
					//								IKafkaConstants.TOPIC_NAME,
						//							true);	
		
		//_writerToFile.persistJsonMsgs(jsonStream.toString());
				
		List<String> jsons = _writerToFile.readJsonMsgsFromFile(offset, numMsgs);
		
		if(jsons == null)
			return false;
		
		System.out.println("(TweetStreamGenerator.runProducer) jsons.size() = "+ jsons.size());
		/**
		 * Degug: Testar se adicionou tudo da janela na KB
		 */
		try{
			FileWriter writer = new FileWriter("jsonFile"+_msgCount+".ttl");
			for(String str: jsons) {
				writer.write(str);
			}
			writer.close();
		}
		catch (Exception e)
		{
			System.out.println("Deu algo errado ai: "+e);
		}

		
		// Envia todas as mensagens json
		timeToBuildJsonMsg = System.currentTimeMillis();
		for(int i=0;i<jsons.size();i++)
		{			
			final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME, jsons.get(i));

			try {
				RecordMetadata metadata = producer.send(record).get();
				System.out.println("Record sent with key " + _msgCount + " to partition " + metadata.partition()
						+ " with offset " + metadata.offset());
				System.out.println("To topic: "+IKafkaConstants.TOPIC_NAME);
				//System.out.println("Record: "+record);
			} catch (ExecutionException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			} catch (InterruptedException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			}
			
			if(_lastSendTime != 0)
				timeToBuildJsonMsg = System.currentTimeMillis() - _lastSendTime;
			else
				timeToBuildJsonMsg = timeToBuildJsonMsg - System.currentTimeMillis();
			
			_writerToFile.writeToStreamGeneratorFileNewLine(timeToBuildJsonMsg.toString(), _msgCount, IKafkaConstants.NUMBER_OF_TRIPLES_PER_MSG);			
			_msgCount++;
			_lastSendTime = System.currentTimeMillis();	
		}
		
		return true;
	}
	
	/**
	 * Debug function to print a query result from a Model.
	 * @param q
	 * @param model
	 */
	private static void outQuery(String q, Model model) 
	{
        Query query = QueryFactory.create(q);
        QueryExecution execution = QueryExecutionFactory.create(query, model);
        ResultSet results = execution.execSelect();
        ResultSetFormatter.out(System.out, results, query);     
        
        try{
        	 FileWriter fstream = new FileWriter("out.txt");
        	 BufferedWriter out = new BufferedWriter(fstream);
        	 
        	 String resultString = ResultSetFormatter.asText(results);
        	 
        	 out.write(resultString);
        	 out.close();
        }
        catch (Exception e){
        	  System.err.println("Error: " + e.getMessage());
        }
        
        execution.close();
    }

}
