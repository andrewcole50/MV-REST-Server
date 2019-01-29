import com.rocketsoftware.mvapi.MVConnection;
import com.rocketsoftware.mvapi.exceptions.MVException;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.util.Properties;

class RocketFactory extends BasePooledObjectFactory<MVConnection> {

	@Override
	public MVConnection create() {
		MVConnection conn = null;
		Properties props = new Properties();
		props.setProperty("username", Resources.MVSP_ACCOUNT);
		props.setProperty("password", Resources.MVSP_PASSWORD);
		try {
			//Live
			System.out.println("    Connecting");
			conn = new MVConnection(Resources.CONN_STRING, props);
			System.out.println("    Connected");
			conn.logTo(Resources.MV_ACCOUNT, Resources.MV_PASSWORD);
			System.out.println("    Logged In");
		} catch (MVException e) {
			System.err.println("========MVConnection Error========");
			System.err.println(e.getMessage());
			System.err.println(e.getErrorCode());
			System.err.println("==================================");
		}
		return conn;
	}

	@Override
	public PooledObject<MVConnection> wrap(MVConnection obj) {
		return new DefaultPooledObject<>(obj);
	}


}
