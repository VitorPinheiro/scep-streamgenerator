package lac.inf.puc.rio.br.streamgenerator;

import java.io.BufferedReader;
import java.util.ArrayList;
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

import eu.larkc.csparql.cep.api.RdfQuadruple;
import lac.inf.puc.rio.br.streamgenerator.utils.IKafkaConstants;
import lac.inf.puc.rio.br.streamgenerator.utils.Utils;;

/**
 * Hello world!
 *
 */
public class StreamGenerator 
{
	private static Utils _utils;
	private static UUID _myUUID;
	private static Integer _msgCount;
	
	public static void main(String[] args) 
	{
		Initialize();
		runProducer();
	}
	
	private static void Initialize()
	{
		_utils = new Utils();
		_myUUID = UUID.fromString("bb103877-8335-444a-be5f-db8d916f6754");
		_msgCount = 0;
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


	private static void runProducer() 
	{
		Producer<Long, String> producer = createProducer();
		
		//BufferedReader input = _utils.readFileAsStream("examples/test2.stream");
		//BufferedReader input = _utils.readFileAsStream("examples/test2.stream");
		//BufferedReader input = _utils.readFileAsStream("examples/consultation.stream");
		BufferedReader input = _utils.readFileAsStream("examples/busEvent.stream");
		ArrayList<RdfQuadruple> triples = _utils.readTriples(input);

		
		JsonObject jsonStream = _utils.toJsonObject(triples, 
													_myUUID.toString(), 
													Integer.toString(_msgCount),
													"IS1",
													false);
		
		final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME, jsonStream.toString());

		System.out.println("Envio isso aqui:");
		System.out.println(jsonStream.toString());


		try {
			RecordMetadata metadata = producer.send(record).get();
			System.out.println("Record sent with key " + _msgCount + " to partition " + metadata.partition()
					+ " with offset " + metadata.offset());
		} catch (ExecutionException e) {
			System.out.println("Error in sending record");
			System.out.println(e);
		} catch (InterruptedException e) {
			System.out.println("Error in sending record");
			System.out.println(e);
		}
		
		_msgCount++;
	}
}
