package demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;
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
        getVillainsNamesAndCountOfMinions();

        //3. Get Minion Names
        getMinionNamesEx();

        //4. Add minion
        addMinionEx();

        //5. Change town names
        changeTownNames();


        //6.Remove Villain
        removeVillainById();

        //7. Print all minion names
        printAllMinionNames();

        //8. Increase minion's age
        increaseMinionsAge();

        //9. Increase Age StoredProcedure
          increaseAgeWithStoredProcedure();

    }

    private static void removeVillainById() throws IOException, SQLException {
        System.out.println("Enter villain id: ");
        String villainId = reader.readLine();
        String villainName = "";
        query = "select name from villains where id = ?";
        statement = connection.prepareStatement(query);
        statement.setString(1,villainId);
        ResultSet rs = statement.executeQuery();
        while(rs.next()){
            villainName = rs.getString("name");
        }
        if(villainName.equals("")){
            System.out.println("No such villain was found");
        }else {
            query = "select count(minion_id) as `count` from minions_villains where villain_id=?";
            statement = connection.prepareStatement(query);
            statement.setString(1, villainId);
            ResultSet rSet = statement.executeQuery();
            int minionCount = 0;
            while (rSet.next()) {
                minionCount = rSet.getInt("count");
            }
            System.out.printf("%s was deleted%n", villainName);
            System.out.printf("%d minions released", minionCount);
            query = "delete from minions_villains where villain_id = ?";
            statement = connection.prepareStatement(query);
            statement.setString(1, villainId);
            statement.executeUpdate();
            query = "delete from villains where id=?";
            statement = connection.prepareStatement(query);
            statement.setString(1, villainId);
            statement.executeUpdate();

        }
    }

    private static void changeTownNames() throws IOException, SQLException {
        System.out.println("Enter country name:");
        String countryName = reader.readLine();
        if(!checkIfCountryExistsByName(countryName, "towns")){
            System.out.println("No town names were affected.");
        }else{
            query = "update towns set name = upper(name) where country =?";
            statement = connection.prepareStatement(query);
            statement.setString(1, countryName);
            statement.execute();
            query = "select name from towns where country = ?";
            statement = connection.prepareStatement(query);
            statement.setString(1, countryName);
            ResultSet rs = statement.executeQuery();
            List towns = new LinkedList();
            while(rs.next()){
                towns.add(rs.getString("name"));
            }
            System.out.printf("%d town names were affected.%n", towns.size());
            System.out.println(towns.toString());
        }
    }

    private static boolean checkIfCountryExistsByName(String entityName,String tableName) throws SQLException {

        query="select * from " + tableName + " where country = ?";
        statement = connection.prepareStatement(query);
        statement.setString(1, entityName);
        ResultSet rs = statement.executeQuery();
        return rs.next();
    }


    private static void printAllMinionNames() throws SQLException {
        query  ="select count(id) as `count` from minions";
        statement = connection.prepareStatement(query);
        ResultSet rs = statement.executeQuery();
        int minionCount = 0;
        while(rs.next()) {
            minionCount = rs.getInt("count");
        }

            for (int i = 0; i <= minionCount/2; i++) {
                query="select name from minions where id in(?,?)";
                statement = connection.prepareStatement(query);
                statement.setInt(1,(1+i));
                statement.setInt(2, (50 -i));

                ResultSet rSet = statement.executeQuery();
                while(rSet.next()){
                    System.out.printf("%s%n",rSet.getString("name"));
                }
            }


    }

    private static void increaseMinionsAge() throws IOException, SQLException {
        System.out.println("Give minions'ids:");
        String[] minionIds =  reader.readLine().split("\\s+");
        for (int i = 0; i < minionIds.length ; i++) {
            query = "update minions set age = (age +1), name = lcase(name) where id=?";
            statement = connection.prepareStatement(query);
            statement.setString(1, minionIds[i]);
            statement.execute();
        }

        query = "select * from minions";
        statement = connection.prepareStatement(query);
        ResultSet rs = statement.executeQuery();
        while(rs.next()){
            System.out.printf("%s %s %s%n",
                    rs.getInt("id"), rs.getString("name"),
                    rs.getInt("age"));
        }

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
            System.out.printf("Town %s was added to the database.%n",minionTown);
        }

        String townId = "";
        query = "select id from towns where name = ?";
        statement = connection.prepareStatement(query);
        statement.setString(1, minionTown);
        ResultSet townResultSet = statement.executeQuery();
        while(townResultSet.next()){
            townId = townResultSet.getString("id");
        }

        insertMinion(minionName,minionAge, townId);

        if(!checkIfEntityExistsByName(villainName,"villains")){
            insertVillain(villainName,"evil");
            System.out.printf("Villain %s was added to the database.%n",villainName);
        }

        String minionId = "";
        query = "select id from minions where name = ?";
        statement = connection.prepareStatement(query);
        statement.setString(1, minionName);
        ResultSet rs = statement.executeQuery();
        while(rs.next()){
            minionId = rs.getString("id");
        }

        String villainId = "";
        query = "select id from villains where name = ?";
        statement = connection.prepareStatement(query);
        statement.setString(1, villainName);
        ResultSet rSet = statement.executeQuery();
        while(rSet.next()){
            villainId = rSet.getString("id");
        }

        query = "insert into minions_villains values(?,?)";
        statement = connection.prepareStatement(query);
        statement.setString(1, minionId);
        statement.setString(2, villainId);
        statement.executeUpdate();
        System.out.printf("Successfully added %s to be minion of %s", minionName, villainName);
    }


    private static void insertMinion(String minionName, int age, String townName) throws SQLException {
        query="insert into minions (name, age, town_id) values (?, ?, ?)";
        statement = connection.prepareStatement(query);
        statement.setString(1, minionName);
        statement.setInt(2, age);
        statement.setString(3, townName);
        statement.executeUpdate();
    }
    private static void insertVillain(String villainName, String evil) throws SQLException {
        query="insert into villains (name, evilness_factor) values (?, ?)";
        statement = connection.prepareStatement(query);
        statement.setString(1, villainName);
        statement.setString(2, evil);
        statement.execute();
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
