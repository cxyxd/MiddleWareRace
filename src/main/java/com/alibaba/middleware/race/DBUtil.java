
package com.alibaba.middleware.race;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import javax.sql.DataSource;

import backtype.storm.tuple.Tuple;

import com.alibaba.middleware.race.model.PaymentMessage;


public class DBUtil {

	private static DataSource ds = null;	
	
	static {
		try{
			InputStream in = DBUtil.class.getClassLoader()
					               .getResourceAsStream("ds.properties");
            Properties props = new Properties();
			props.load(in);
	//		ds = DruidDataSourceFactory.createDataSource(props);
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	public static Connection openConnection() throws SQLException{
		return ds.getConnection();
	}	
	
	@SuppressWarnings("unchecked")
	public synchronized static void wirteMySQL(Tuple tuple) {
		
		String id = tuple.getSourceStreamId();
		if (id.equals("stop")) 
			return;
		Connection con=null;
		PreparedStatement ps=null;
		List<PaymentMessage> list=(List<PaymentMessage>) tuple.getValueByField("list");
		try {
			con=DBUtil.openConnection();
			con.setAutoCommit(false);
			ps = con.prepareStatement("insert into pay values(?,?,?,?,?)");
			for (PaymentMessage pay : list) {
				long createTime = (pay.getCreateTime() / 1000 / 60) * 60;
				ps.setLong(1, createTime);
				ps.setDouble(2, pay.getPayAmount());
				ps.setString(3, id);
				ps.setString(4, "" + pay.getOrderId());
				ps.setInt(5, pay.getPayPlatform());
				ps.addBatch();
			}
			ps.executeBatch();
			con.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			try {
				con.setAutoCommit(true);
				ps.close();
				con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
		

	}
}