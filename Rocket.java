import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.rocketsoftware.mvapi.MVConnection;
import com.rocketsoftware.mvapi.MVConstants;
import com.rocketsoftware.mvapi.MVSubroutine;
import com.rocketsoftware.mvapi.ResultSet.MVResultSet;
import com.rocketsoftware.mvapi.exceptions.MVException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class Rocket {

	private MVConnection mvConn;

	Rocket() throws Exception {
		this.mvConn = Main.rPool.borrowObject();
	}

	String read(String filename, String key) throws MVException {
		MVSubroutine sub = mvConn.mvSub("JREAD", 4);
		sub.setArg(0, filename);
		sub.setArg(1, key);
		sub.mvCall();

		if (sub.getArg(3).equals("1")) {
			return Main.gson.toJson(false);
		} else {
			return Main.gson.toJson(MVtoJSON(sub.getArg(2), key));
		}
	}

	String readu(String filename, String key) throws MVException {
		MVSubroutine sub = mvConn.mvSub("JREADU", 5);
		sub.setArg(0, filename);
		sub.setArg(1, key);
		sub.mvCall();

		if (sub.getArg(3).equals("1") || !sub.getArg(4).equals("0")) {
			return Main.gson.toJson(false);
		} else {
			return Main.gson.toJson(MVtoJSON(sub.getArg(2), key));
		}
	}

	String write(String filename, String key, String jsonString) throws MVException {
		String record = JSONtoMV(jsonString);
		MVSubroutine sub = mvConn.mvSub("JWRITE", 4);
		sub.setArg(0, filename);
		sub.setArg(1, key);
		sub.setArg(2, record);
		sub.mvCall();
		return Main.gson.toJson(!Boolean.valueOf(sub.getArg(3)));
	}

	String delete(String filename, String key) throws MVException {
		mvConn.fileDelete(filename, key);
		return Main.gson.toJson(true);
	}

	String release(String filename, String key) throws MVException {
		mvConn.fileRelease(filename, key);
		return Main.gson.toJson(true);
	}

	String gosub(String subName, JsonArray jsonParams, int length) throws MVException {
		MVSubroutine sub = mvConn.mvSub(subName, length);

		for (int i = 0; i < length; i++) {
			if (i < jsonParams.size()) {
				if (jsonParams.get(i).isJsonArray()) {
					sub.setArg(i, JSONtoMV(jsonParams.get(i).toString()));
				} else {
					sub.setArg(i, jsonParams.get(i).toString());
				}
			} else {
				sub.setArg(i, "");
			}
		}

		sub.mvCall();

		JsonArray response = new JsonArray();
		for (int i = 0; i < length; i++) {
			if (sub.getArg(i).contains(MVConstants.AM)) {
				response.add(MVtoJSON(sub.getArg(i), ""));
			} else {
				response.add(new JsonPrimitive(sub.getArg(i)));
			}
		}

		return Main.gson.toJson(response);
	}

	String execute(String query) throws MVException {
		MVResultSet rs = mvConn.executeQuery(query);
		List<List<String>> resultSet = new ArrayList<>();
		while (rs.next()) {
			resultSet.add(new ArrayList<>(Arrays.asList(rs.getCurrentRow().split(MVConstants.AM))));
		}

		return Main.gson.toJson(resultSet);
	}

	void close() throws Exception {
		Main.rPool.returnObject(this.mvConn);
	}

	private JsonArray MVtoJSON(String mvRecord, String key) {
		JsonArray jsonArray = new JsonArray();
		if (!"".equals(key)) {
			jsonArray.add(new JsonPrimitive(key));
		}

		String[] rawRecord = mvRecord.split(MVConstants.AM);

		for (String attribute : rawRecord) {
			if (attribute.contains(MVConstants.VM)) {
				JsonArray jsonValues = new JsonArray();
				String[] values = attribute.split(MVConstants.VM);
				for (String value : values) {
					if (value.contains(MVConstants.SM)) {
						jsonValues.add(parseSubValues(value));
					} else {
						jsonValues.add(new JsonPrimitive(value));
					}
				}
				jsonArray.add(jsonValues);
			} else {
				if (attribute.contains(MVConstants.SM)) {

					JsonArray values = new JsonArray();
					values.add(parseSubValues(attribute));
					jsonArray.add(values);
				} else {
					jsonArray.add(new JsonPrimitive(attribute));
				}
			}
		}


		return new JsonArray();
	}

	private JsonArray parseSubValues(String input) {
		JsonArray jsonSubValues = new JsonArray();
		String[] subvalues = input.split(MVConstants.SM);
		for (String subvalue : subvalues) {
			jsonSubValues.add(new JsonPrimitive(subvalue));
		}
		return jsonSubValues;
	}

	private String JSONtoMV(String jsonString) {
		JsonArray jsonArray = (JsonArray) new JsonParser().parse(jsonString);
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < jsonArray.size(); i++) {
			JsonElement attribute = jsonArray.get(i);
			if (attribute.isJsonArray()) {
				JsonArray values = attribute.getAsJsonArray();
				for (int j = 0; j < values.size(); j++) {
					JsonElement value = values.get(j);
					if (value.isJsonArray()) {
						JsonArray subValues = value.getAsJsonArray();
						for (int k = 0; k < subValues.size(); k++) {
							sb.append(subValues.get(k).getAsString());

							if (k < subValues.size() - 1) sb.append(MVConstants.SM);
						}
					} else {
						sb.append(value.getAsString());
					}

					if (j < values.size()) sb.append(MVConstants.VM);
				}
			} else {
				sb.append(attribute.getAsString());
			}

			if (i < jsonArray.size() - 1) sb.append(MVConstants.AM);
		}

		return sb.toString();
	}

}
