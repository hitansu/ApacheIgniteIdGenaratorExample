package com.hitansu.ignite.idgen;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class IdPersistService {

  static Connection conn= null;
//  static Map<String, Connection> conPool= new HashMap<String, Connection>();

	public static void main(String[] args) {
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		IdPersistService obj= new IdPersistService();
	//	obj.persistId("xxxfffffffffff", 23332);
		obj.checkIfIdPresentInDb("BUG-");
	}
	
	public boolean persistId(String key, long id) {
		String table_name= "IDTICKET";
		String schema_name= "JENA";
		String insert_query= "INSERT INTO "+schema_name+"."+table_name+" (key, id) SELECT '"+key+"' ,"+id+" FROM DUAL WHERE NOT EXISTS (SELECT key FROM "+schema_name+"."+table_name+" WHERE key='"+key+"')";
		String update_query= "UPDATE "+schema_name+"."+table_name+" SET id="+id+" WHERE key='"+key+"'";
		
	//	String insert_query= "INSERT INTO "+schema_name+"."+table_name+" (key, id) VALUES('"+key+"', "+id+")";
		Statement createStatement= null;
		Statement updateStatement= null;
		try {
			if(/*conPool.get(key)== null*/conn== null) {
				conn= DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:orcl", "jena", "jena");
				//conPool.put(key, conn);
			}
		//	conn= conPool.get(key);	
			conn.setAutoCommit(false);
			createStatement= conn.createStatement();
		//	updateStatement= conn.createStatement();
			createStatement.addBatch(insert_query);
			createStatement.addBatch(update_query);
			//createStatement.execute(insert_query);
			//updateStatement.executeQuery(update_query);
			createStatement.executeBatch();
			conn.commit();
			conn.setAutoCommit(true);
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			if(updateStatement!= null) {
				try {
					updateStatement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(createStatement!= null) {
				try {
					createStatement.clearBatch();
					createStatement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(conn!= null) {/*
				try {
				//	conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			*/}
		}
	}
	
	public long checkIfIdPresentInDb(String key) {
		String table_name= "IDTICKET";
		String schema_name= "JENA";
		String select_query= "SELECT id FROM "+schema_name+"."+table_name+" WHERE key='"+key+"'";
		Statement st= null;
		long id= -1;
		try {
			if(/*conPool.get(key)== null*/conn== null) {
				conn= DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:orcl", "jena", "jena");
			//	conPool.put(key, conn);
			}
		//	conn= conPool.get(key);
			st= conn.createStatement();
			ResultSet rs = st.executeQuery(select_query);
			if(rs.next()) {
				id= rs.getLong(1);
			}
		} catch(SQLException e) {
			
		} finally {
			if(st!= null) {
				try {
					st.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return id;
	}
	
	public void closeAllConn() {
		if(conn!= null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		/*
		Set<String> keySet = conPool.keySet();
		for(String key: keySet) {
			if(conPool.get(key)!= null) {
				try {
					conPool.get(key).close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	*/}
}
