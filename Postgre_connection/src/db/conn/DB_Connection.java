package db.conn;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.postgresql.util.PSQLException;

public class DB_Connection {
	
	static String url;
	static Connection conn;
	static Statement stmt;
	static PreparedStatement p_stmt;
	static ResultSet rslt_set;
	static String ip;
	static String db_name;
	static String user ;
	static String password ;
	
	public static void main(String[]args) throws SQLException {
		
		
		ip = "192.168.1.106";
		db_name = "postgres";
		user = "postgres";
		password = "XpgGigabyte" ;
		do_connection(ip, db_name, user, password);
		
		String[] att_val_and_type = {"A VARCHAR","B VARCHAR","C VARCHAR", "D VARCHAR"};
		//new_table("Prova_new", att_val_and_type );
		
		String[] name_attr_new_tupla = {"A","B","C","D"};
		String[] val_attr_new_tupla = {"ron","ken","son","shi"};
		//new_tupla("Prova_new", name_attr_new_tupla, val_attr_new_tupla );
		
		//drop_table("Prova_new");
		
		String[] name_attr_read_table = {"*"};
		//read_table(name_attr_read_table, "Prova_new");
		
	}
	
	
	public static void do_connection(String ip, String db_name, String user, String password) throws SQLException {
		url = "jdbc:postgresql://"+ip+"/"+db_name+"?user="+user+"&password="+password;
		conn = DriverManager.getConnection(url);
		if(conn.isValid(100)) {
			System.out.println("Connessione stabilita");
			stmt = conn.createStatement();
		}else {
			System.out.println("Connessione fallita");
			System.exit(0);
		}	
		
	}
	
	
	public void do_query(String query) throws SQLException {stmt.executeQuery(query);}
	
	
	
	/**
	 * 
	 * @param name_table nome della tabella che si sta creando
	 * @param att_val_and_type necessario inserire nome attributo seguito dal suo tipo es. Codice INT
	 */
	public static void new_table(String name_table, String [] att_val_and_type) {
	
		String table = "CREATE TABLE "+name_table+" (";
		for(int i = 0 ; i < att_val_and_type.length-1; i++ ) {
			
			table += att_val_and_type[i]+", ";
		}
		table += att_val_and_type[att_val_and_type.length-1]+")";
		System.out.println(table);
		try {
			conn.createStatement().execute(table);
			System.out.println("Tabella creata");
			
		} catch (SQLException e) {
				System.out.println("La tabella è già esistente");
		}

	}
	
	
	/**
	 * futura implementazione : verificare esistenza condizione WHERE
	 * @param name_table nome della tabella che si vuole eliminare
	 * @throws SQLException tramite try catch si gestisce l'eccezione che notifica l'inesistenza della tabella indicata. 
	 * Se tale eccezzione si manifesta allora la tabella è stata cancellata
	 */
	
	public static void drop_table(String name_table) throws SQLException {
		String query = "DROP TABLE "+ name_table ;
		try{stmt.executeQuery(query);}catch( PSQLException e) {
			System.out.println("Tabella "+ name_table +" cancellata");
		}	
	}
	
	/**
	 * Il metodo permette la modifica di un attributo dove è rispettata la condizione indicata dal valore di un'altro attributo facente parte della medesima tabella. 
	 * @param name_table nome della tabella nella quale è presente l'attributo che si vuole modificare
	 * @param name_attr_1 nome attributo da modificare
	 * @param val_attr_1 nuovo valore dell'attributo
	 * @param name_attr_2 nome attributo per condizionale
	 * @param val_attr_2 valore dell'attributo per condizionale
	 * @throws SQLException
	 */
	public void update_table(String name_table, String name_attr_1, Object val_attr_1, String name_attr_2, Object val_attr_2) throws SQLException {
		
		String value = "UPDATE "+name_table+" SET "+name_attr_1+" = "+val_attr_1+" WHERE "+name_attr_2+" = "+val_attr_2;
		stmt.executeQuery(value);

		System.out.println("update first row ! ");

	}
	
	/**
	 * Il metodo permette di inserire una nuova tupla all'interno della tabela indicata.
	 * La query viene adattata in base dati inviati al metodo i quali non devono presentare segni di punteggiatura o qualisiasi altro elemento al di fuori del loro valore o nome.
	 * @param name_table nome della tabella della quale si vuole inserire la nuova tupla
	 * @param name_att nome dell'attributo o attributi presenti nella tabella indicata e dei quali si vuole assegnare un valore
	 * @param val_att valore che si vuole assegnare agli attributi
	 * @throws SQLException
	 */
	public static void new_tupla(String name_table, String[] name_att, String[]val_att) throws SQLException {
		String list_val_att = "";
		String list_att = "";
		for(int j =0 ; j<name_att.length-1;j++) {
			list_val_att +="?,";
			list_att += name_att[j]+",";
		}
		list_val_att += "?";
		list_att+= name_att[name_att.length-1];
		
		System.out.println(list_val_att);
		System.out.println(list_att);
		
		String value = "INSERT INTO Prova_new("+list_att+")"
				+ "VALUES" +"("+list_val_att+")";
		PreparedStatement p_stmt = conn.prepareStatement(value);
		for(int i = 0; i<name_att.length;i++) {
			
			p_stmt.setString(i+1, val_att[i]);
			
		}
		p_stmt.executeUpdate();

		System.out.println("add new row !");

	}
	
	@SuppressWarnings("null")
	public static void read_table(String[] attribute ,String name_table) throws SQLException {
		String attr_list = "";
		for(int i = 0; i< attribute.length-2; i++) {
			
			attr_list += attribute[i]+",";
		}
		attr_list += attribute[attribute.length-1];
		
		rslt_set = stmt.executeQuery("SELECT "+attr_list+" FROM "+name_table);
		
		ResultSetMetaData rslt_set_mtdt = rslt_set.getMetaData();
		//lettura nome attributi 
		int clmn_lnght = rslt_set_mtdt.getColumnCount();
		ArrayList<String> name_column = new ArrayList<String>();
		for(int i = 1; i<=clmn_lnght;i++) {
			name_column.add(rslt_set_mtdt.getColumnName(i));
			System.out.print(name_column.get(i-1)+" | ");
		}
		System.out.println();
		while(rslt_set.next()){
			for(int j=0; j<clmn_lnght;j++) {
				System.out.println(rslt_set.getObject(name_column.get(j)));
			}
			
		}
		System.out.println();
		System.out.println("read row ! ");
		System.out.println("--------------------------");
	}
	
	
	
	
}
