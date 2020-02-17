package demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.Properties;

public class Main {

    private static final String CONNECTION_STRING =
            "jdbc:mysql://localhost:3306/";
    private static final String DATABASE_NAME = "minions_db";

    private static Connection connection;

    private static String query;

    private static PreparedStatement statement;

    private static BufferedReader reader;

    public static void main(String[] args) throws SQLException, IOException {

        reader = new BufferedReader(new InputStreamReader((System.in)));
        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", "12345");

        connection = DriverManager
                .getConnection(CONNECTION_STRING + DATABASE_NAME, properties);

        // 2. Get Villains' Names
//        getVillainsNamesAndCountOfMinions();

        //3. Get Minion Names
//        getMinionNamesEx();

        //4. Add minion
//        addMinionEx();

        //9. Increase Age StoredProcedure
          increaseAgeWithStoredProcedure();

    }

    private static void increaseAgeWithStoredProcedure() throws IOException, SQLException {
        System.out.println("Enter minion id: ");
        int minionId = Integer.parseInt(reader.readLine());
        query="call usp_get_older(?)";
        CallableStatement callableStmt = connection.prepareCall(query);

        callableStmt.setInt(1, minionId);
        callableStmt.execute();

    }

    private static void addMinionEx() throws IOException, SQLException {
        System.out.println("Enter minion parameters: ");
        String[] minionParameters = reader.readLine().split("\\s+");
        String minionName = minionParameters[0];
        int minionAge = Integer.parseInt(minionParameters[1]);
        String minionTown = minionParameters[2];
        System.out.println("Enter villain name: ");
        String villainName = reader.readLine();
        if(!checkIfEntityExistsByName(minionTown,"towns")){
            insertEntityInTown(minionTown);
        }


    }

    private static void insertEntityInTown(String minionTown) throws SQLException {
        query="insert into towns (name, country) values (?, ?)";
        statement = connection.prepareStatement(query);
        statement.setString(1, minionTown);
        statement.setString(2, "NULL");
        statement.execute();
    }

    private static boolean checkIfEntityExistsByName(String entityName,String tableName) throws SQLException {

        query="select * from " + tableName + " where name = ?";
        statement = connection.prepareStatement(query);
        statement.setString(1, entityName);
        ResultSet rs = statement.executeQuery();
        return rs.next();
    }

    private static void getMinionNamesEx() throws IOException, SQLException {
        System.out.println("Enter villain id:");
        int villain_id=Integer.parseInt((reader.readLine()));

        if(!checkIfEntityExists(villain_id, "villains")){
            System.out.printf("No villain with ID %d exists in the database.", villain_id);
            return;
        }
        System.out.printf("Villain: %s%n", getEntityNameById(villain_id, "villains"));
    getMinionsAndAgeByVillainId(villain_id);
    }

    private static void getMinionsAndAgeByVillainId(int villain_id) throws SQLException {
        query="select m.name, m.age from minions as m \n" +
                "join minions_villains mv on m.id = mv.minion_id\n" +
                "where mv.villain_id = ?;";

        statement = connection.prepareStatement(query);
        statement.setInt(1,villain_id);
        ResultSet rs = statement.executeQuery();
        int minionNumber = 0;
        while(rs.next()){
            System.out.printf("%d. %s %d%n", ++minionNumber,rs.getString("name"),
                    rs.getInt(2));

        }
    }

    private static String getEntityNameById(int entityId, String tableName) throws SQLException {

        query="select name from "+ tableName + " where id = ?";
        statement = connection.prepareStatement(query);
        statement.setInt(1, entityId);

        ResultSet rs = statement.executeQuery();

        return  rs.next() ? rs.getString("name"): null;
    }

    private static boolean checkIfEntityExists(int villain_id, String villains) throws SQLException {
    query="select * from " + villains + " where id = ?";

    statement = connection.prepareStatement(query);
    statement.setInt(1, villain_id);
    ResultSet rs = statement.executeQuery();
    return  rs.next();
    }

    private static void getVillainsNamesAndCountOfMinions() throws SQLException {
        query = "select v.name, count(mv.minion_id) as 'count'\n" +
                "from villains as v JOIN\n" +
                "minions_villains mv on v.id=mv.villain_id\n" +
                "group by v.name\n" +
                "having `count`> 15\n" +
                "order by `count` desc";

        statement = connection.prepareStatement(query);
        ResultSet resultSet = statement.executeQuery();

        while (resultSet.next()) {
            System.out.printf("%s %d%n", resultSet.getString("name"),
                    resultSet.getInt("count"));
        }
    }
}
