import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.jbase.jremote.*;

import java.util.ArrayList;
import java.util.List;

class Jbase {

	private JConnection jconn;

	Jbase() throws Exception {
		this.jconn = Main.jPool.borrowObject();
	}

	String read(String filename, String key) throws JRemoteException {
		JSubroutineParameters params = new JSubroutineParameters();
		params.add(new JDynArray(filename));
		params.add(new JDynArray(key));
		params.add(new JDynArray(""));
		params.add(new JDynArray(""));

		JSubroutineParameters returnParams = this.jconn.call("JREAD", params);
		if (returnParams.get(3).get(1).equals("1")) {
			return Main.gson.toJson(false);
		} else {
			return Main.gson.toJson(MVtoJSON(returnParams.get(2), key));
		}
	}

	String readu(String filename, String key) throws JRemoteException {
		JSubroutineParameters params = new JSubroutineParameters();
		params.add(new JDynArray(filename));
		params.add(new JDynArray(key));
		params.add(new JDynArray(""));
		params.add(new JDynArray(""));
		params.add(new JDynArray(""));

		JSubroutineParameters returnParams = this.jconn.call("JREADU", params);
		if (returnParams.get(3).get(1).equals("1") || !returnParams.get(4).get(1).equals("0")) {
			return Main.gson.toJson(false);
		} else {
			return Main.gson.toJson(MVtoJSON(returnParams.get(2), key));
		}
	}

	String write(String filename, String key, String jsonString) throws JRemoteException {
		JDynArray jRecord = JSONtoMV(jsonString);
		JSubroutineParameters params = new JSubroutineParameters();
		params.add(new JDynArray(filename));
		params.add(new JDynArray(key));
		params.add(jRecord);
		params.add(new JDynArray(""));

		JSubroutineParameters returnParams = jconn.call("JWRITE", params);
		return Main.gson.toJson(!Boolean.valueOf(returnParams.get(3).get(0)));
	}

	String delete(String filename, String key) throws JRemoteException {
		JFile jFile = jconn.open(filename);
		String jsonResponse = Main.gson.toJson(jFile.delete(key));
		jFile.close();
		return jsonResponse;
	}

	String release(String filename, String key) throws JRemoteException {
		JFile jFile = jconn.open(filename);
		return Main.gson.toJson(jFile.releaseLock(key));
	}

	String gosub(String subName, JsonArray jsonParams, int length) throws JRemoteException {
		JSubroutineParameters params = new JSubroutineParameters();
		for (int i = 0; i < length; i++) {
			if (i < jsonParams.size()) {
				if (jsonParams.get(i).isJsonArray()) {
					params.add(JSONtoMV(jsonParams.get(i).toString()));
				} else {
					params.add(new JDynArray(jsonParams.get(i).toString()));
				}
			} else {
				params.add(new JDynArray(""));
			}
		}

		JsonArray response = new JsonArray();
		JSubroutineParameters returnParams = jconn.call(subName, params);
		for (int i = 0; i < length; i++) {
			if (returnParams.get(i).getNumberOfAttributes() == 1) {
				response.add(new JsonPrimitive(returnParams.get(i).get(1)));
			} else {
				response.add(MVtoJSON(returnParams.get(i), ""));
			}
		}
		return Main.gson.toJson(response);
	}

	String execute(String query) throws JRemoteException {
		JResultSet rs = jconn.createStatement().execute(query);
		List<String> resultSet = new ArrayList<>();
		while (rs.next()) {
			System.out.println(rs.getRow().getNumberOfAttributes());
			resultSet.add(rs.getRow().get(1));
		}
		System.out.println(resultSet.toString());
		return Main.gson.toJson(resultSet);
	}

	void close() throws Exception {
		Main.jPool.returnObject(this.jconn);
	}

	private JsonArray MVtoJSON(JDynArray mvRecord, String key) {
		JsonArray jsonArray = new JsonArray();
		if (!"".equals(key)) {
			jsonArray.add(new JsonPrimitive(key));
		}
		JsonArray values;
		JsonArray subValues;

		for (int i=1; i<=mvRecord.getNumberOfAttributes(); i++) {
			if (mvRecord.getNumberOfValues(i) > 1) {
				values = new JsonArray();
				for (int j=1; j<=mvRecord.getNumberOfValues(i); j++) {
					if (mvRecord.getNumberOfSubValues(i, j) > 1) {
						subValues = new JsonArray();
						for (int k=1; k<=mvRecord.getNumberOfSubValues(i, j); k++) {
							subValues.add(new JsonPrimitive(mvRecord.get(i, j, k)));
						}
						values.add(subValues);
					} else {
						values.add(new JsonPrimitive(mvRecord.get(i, j)));
					}
				}
				jsonArray.add(values);
			} else {
				if (mvRecord.getNumberOfSubValues(i,1) > 1) {
					values = new JsonArray();
					subValues = new JsonArray();
					for (int j=1; j<=mvRecord.getNumberOfSubValues(i, 1); j++) {
						subValues.add(new JsonPrimitive(mvRecord.get(i, 1, j)));
					}
					values.add(subValues);
					jsonArray.add(values);
				} else {
					jsonArray.add(new JsonPrimitive(mvRecord.get(i)));
				}
			}
		}
		return jsonArray;
	}

	private JDynArray JSONtoMV(String jsonString) {
		JsonArray jsonArray = (JsonArray) new JsonParser().parse(jsonString);

		JDynArray mvRecord = new JDynArray();
		for (int i = 1; i < jsonArray.size(); i++) {
			if (jsonArray.get(i).isJsonArray()) {
				JsonArray values = jsonArray.get(i).getAsJsonArray();
				for (int j = 0; j < values.size(); j++) {
					if (values.get(j).isJsonArray()) {
						JsonArray subvalues = values.get(j).getAsJsonArray();
						for (int k = 0; k < subvalues.size(); k++) {
							mvRecord.replace(subvalues.get(k).getAsString(), i, j+1, k+1);
						}
					} else {
						mvRecord.replace(values.get(j).getAsString(), i, j+1);
					}
				}
			} else{
				mvRecord.replace(jsonArray.get(i).getAsString(), i);
			}
		}

		return mvRecord;
	}
}
