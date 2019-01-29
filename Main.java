import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.jbase.jremote.JConnection;
import com.rocketsoftware.mvapi.MVConnection;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class Main {

	static Gson gson;
	static ObjectPool<JConnection> jPool;
	static ObjectPool<MVConnection> rPool;


	public static void main(String[] args) {
		gson = new Gson();

		System.out.println("Press Ctrl-C to Shut Down Server");
		System.out.println("System Initializing...");

		Runtime.getRuntime().addShutdownHook(new Thread(Main::shutdownServer, "Shutdown-thread"));

		System.out.println("    Initializing MvDBMS connections");
		GenericObjectPoolConfig config = new GenericObjectPoolConfig();
		config.setMaxTotal(Resources.NUM_OF_LINES);

		switch (Resources.PLATFORM) {
			case Resources.JBASE:
				jPool = new GenericObjectPool<>(new JbaseFactory(), config);
				break;
			case Resources.MVBASE:
			case Resources.D3:
			case Resources.U2:
				rPool = new GenericObjectPool<>(new RocketFactory(), config);
				break;
		}

		spark.Spark.port(Resources.SERVER_PORT);

		System.out.println("Server Initialized and Ready");

		spark.Spark.get("/CRUD/:fileName/:key", (request, response) -> {
			String filename = request.params(":fileName");
			String key = request.params(":key");

			String jsonResponse = "";
			try {
				switch (Resources.PLATFORM) {
					case Resources.JBASE:
						Jbase jbase = new Jbase();
						jsonResponse = jbase.read(filename, key);
						jbase.close();
						break;
					case Resources.MVBASE:
					case Resources.D3:
					case Resources.U2:
						Rocket rocket = new Rocket();
						jsonResponse = rocket.read(filename, key);
						rocket.close();
						break;
				}
			} catch (Exception e) {
				jsonResponse = gson.toJson(e.getMessage());
			}


			return jsonResponse;
		});

		spark.Spark.get("/CRUD/:fileName/:key/:lock", (request, response) -> {
			String filename = request.params(":fileName");
			String key = request.params(":key");

			String jsonResponse = "";
			try {
				switch (Resources.PLATFORM) {
					case Resources.JBASE:
						Jbase jbase = new Jbase();
						jsonResponse = jbase.readu(filename, key);
						jbase.close();
						break;
					case Resources.MVBASE:
					case Resources.D3:
					case Resources.U2:
						Rocket rocket = new Rocket();
						jsonResponse = rocket.readu(filename, key);
						rocket.close();
						break;
				}
			} catch (Exception e) {
				jsonResponse = gson.toJson(e.getMessage());
			}


			return jsonResponse;
		});

		spark.Spark.post("/CRUD/:fileName/:key", (request, response) -> {
			String filename = request.params(":fileName");
			String key = request.params(":key");

			String jsonResponse = "";

			switch (Resources.PLATFORM) {
				case Resources.JBASE:
					Jbase jbase = new Jbase();
					jsonResponse = jbase.write(filename, key, request.body());
					jbase.close();
					break;
				case Resources.MVBASE:
				case Resources.D3:
				case Resources.U2:
					Rocket rocket = new Rocket();
					jsonResponse = rocket.write(filename, key, request.body());
					rocket.close();
					break;
			}

			return jsonResponse;
		});

		spark.Spark.delete("/CRUD/:fileName/:key", (request, response) -> {
			String filename = request.params(":fileName");
			String key = request.params(":key");

			String jsonResponse = "";

			switch (Resources.PLATFORM) {
				case Resources.JBASE:
					Jbase jbase = new Jbase();
					jsonResponse = jbase.delete(filename, key);
					jbase.close();
					break;
				case Resources.MVBASE:
				case Resources.D3:
				case Resources.U2:
					Rocket rocket = new Rocket();
					jsonResponse = rocket.delete(filename, key);
					rocket.close();
					break;
			}

			return jsonResponse;
		});

		spark.Spark.patch("/CRUD/:fileName/:key", (request, response) -> {
			String filename = request.params(":fileName");
			String key = request.params(":key");

			String jsonResponse = "";

			switch (Resources.PLATFORM) {
				case Resources.JBASE:
					Jbase jbase = new Jbase();
					jsonResponse = jbase.release(filename, key);
					jbase.close();
					break;
				case Resources.MVBASE:
				case Resources.D3:
				case Resources.U2:
					Rocket rocket = new Rocket();
					jsonResponse = rocket.release(filename, key);
					rocket.close();
					break;
			}
			return jsonResponse;
		});

		spark.Spark.get("/SUBROUTINE/:subName/:length", (request, response) -> {
			String subName = request.params(":subName");
			int length = Integer.valueOf(request.params(":length"));
			JsonArray jsonArray = (JsonArray) new JsonParser().parse(request.body());

			String jsonResponse = "";

			switch (Resources.PLATFORM) {
				case Resources.JBASE:
					Jbase jbase = new Jbase();
					jsonResponse = jbase.gosub(subName, jsonArray, length);
					jbase.close();
					break;
				case Resources.MVBASE:
				case Resources.D3:
				case Resources.U2:
					Rocket rocket = new Rocket();
					jsonResponse = rocket.gosub(subName, jsonArray, length);
					rocket.close();
					break;
			}

			return jsonResponse;
		});

		spark.Spark.get("/EXECUTE", (request, response) -> {
			String query = request.body();
			String jsonResponse = "";

			switch (Resources.PLATFORM) {
				case Resources.JBASE:
					Jbase jbase = new Jbase();
					jsonResponse = jbase.execute(query);
					jbase.close();
					break;
				case Resources.MVBASE:
				case Resources.D3:
				case Resources.U2:
					Rocket rocket = new Rocket();
					jsonResponse = rocket.execute(query);
					rocket.close();
					break;
			}

			return jsonResponse;
		});

	}

	private static void shutdownServer() {
		System.out.println("==============================");
		System.out.println("SHUTTING DOWN THE SERVER!!!!!!");

		ObjectPool pool = null;
		switch (Resources.PLATFORM) {
			case Resources.JBASE:
				pool = jPool;
				break;
			case Resources.MVBASE:
			case Resources.D3:
			case Resources.U2:
				pool = rPool;
				break;
		}

		if (pool != null) {
			while (pool.getNumIdle() > 0 || pool.getNumActive() > 0) {
				System.out.println("Connections  Idle:  " + pool.getNumIdle() + "    Active:  " + pool.getNumActive());
				try {
					switch (Resources.PLATFORM) {
						case Resources.JBASE:
							JConnection jConn = (JConnection) pool.borrowObject();
							jConn.close();
							pool.invalidateObject(jConn);
							break;
						case Resources.MVBASE:
						case Resources.D3:
						case Resources.U2:
							MVConnection rConn = (MVConnection) pool.borrowObject();
							rConn.close();
							pool.invalidateObject(rConn);
							break;
					}
				} catch (Exception e) {
					//e.printStackTrace();
				}
			}
		}

		System.out.println("System is currently OFFLINE");
	}

}
