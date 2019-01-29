import com.jbase.jremote.DefaultJConnectionFactory;
import com.jbase.jremote.JConnection;
import com.jbase.jremote.JRemoteException;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

class JbaseFactory extends BasePooledObjectFactory<JConnection> {

	@Override
	public JConnection create() {
		JConnection conn = null;
		try {
			//Live
			DefaultJConnectionFactory cxf = new DefaultJConnectionFactory();
			cxf.setHost(Resources.HOST);
			cxf.setPort(Resources.PORT);
			System.out.println("    Connecting");
			conn = cxf.getConnection(Resources.MV_ACCOUNT, Resources.MV_PASSWORD);
			System.out.println("    Connected");
		} catch (JRemoteException e) {
			System.err.println("==========JRemote Error===========");
			System.err.println(e.getMessage());
			System.err.println(e.getError());
			System.err.println("==================================");
		}
		return conn;
	}

	@Override
	public PooledObject<JConnection> wrap(JConnection obj) {
		return new DefaultPooledObject<>(obj);
	}

}
