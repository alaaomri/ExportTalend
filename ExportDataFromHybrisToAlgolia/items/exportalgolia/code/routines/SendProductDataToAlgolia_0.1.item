package routines;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.algolia.search.APIClient;
import com.algolia.search.ApacheAPIClientBuilder;
import com.algolia.search.Index;
import com.algolia.search.exceptions.AlgoliaException;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SendProductDataToAlgolia {

	
	public static String SendFullData(String appName, String apikey,
			String fileName, String indiceName) throws IOException,
			AlgoliaException {
		APIClient client = new ApacheAPIClientBuilder(appName, apikey).build();
		Index<ProductsPojo> index = client.initIndex(indiceName,
				ProductsPojo.class);
		index.addObjects(parseData(fileName));
		return "Data exported to Algolia with In Full Mode Success";
	}
	
	public static String SendDiffData(String appName, String apikey,
			String fileName, String indiceName) throws IOException,
			AlgoliaException {
		APIClient client = new ApacheAPIClientBuilder(appName, apikey).build();
		Index<ProductsPojo> index = client.initIndex(indiceName,
				ProductsPojo.class);
		index.partialUpdateObjects(convertToObjects(parseData(fileName)));
		return "Data exported to Algolia In Diff Mode with Success";
	}
	
	protected static List<ProductsPojo> parseData(String filePath) throws IOException {

		InputStream input = new FileInputStream(filePath);
		JsonFactory factory = new JsonFactory();
		List<ProductsPojo> list = new ArrayList<ProductsPojo>();
		ObjectMapper mapper = new ObjectMapper(factory);
		JsonNode rootNode = mapper.readTree(input);
		Iterator<Map.Entry<String, JsonNode>> fieldsIterator = rootNode
				.fields();

		while (fieldsIterator.hasNext()) {
			Map.Entry<String, JsonNode> field = fieldsIterator.next();
			Iterator<Map.Entry<String, JsonNode>> secondfieldsIterator = field
					.getValue().fields();
			while (secondfieldsIterator.hasNext()) {
				Map.Entry<String, JsonNode> secondfield = secondfieldsIterator
						.next();
				if (secondfield.getKey().equals("docs")) {
					Iterator<JsonNode> iterator = secondfield.getValue()
							.elements();
					while (iterator.hasNext()) {
						JsonNode field1 = iterator.next();
						ProductsPojo pp = mapper.convertValue(field1,
								ProductsPojo.class);
						list.add(pp);
					}
				}
			}
		}
		return list;
	}
	
	protected static List<Object> convertToObjects(List<ProductsPojo> products){
		List<Object> objects = new ArrayList<>();
		for(ProductsPojo product:products){
			objects.add((Object)product);
		}
		return objects;
	}
}
