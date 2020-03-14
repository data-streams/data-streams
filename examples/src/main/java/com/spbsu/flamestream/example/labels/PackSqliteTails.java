package com.spbsu.flamestream.example.labels;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public enum PackSqliteTails {
  ;

  public static void main(String[] args) throws Exception {
    try (
            final FileOutputStream indexFile = new FileOutputStream("edges_index.bin");
            final BufferedOutputStream outputStream = new BufferedOutputStream(indexFile, 1 << 20);
            final DataOutputStream indexOutput = new DataOutputStream(outputStream)
    ) {
      // create a database connection
      final Connection connection = DriverManager.getConnection("jdbc:sqlite:twitter.sqlite");
      final Statement statement = connection.createStatement();
      statement.setQueryTimeout(30);  // set timeout to 30 sec.
      final ResultSet rs = statement.executeQuery(
              "SELECT tail, SUM(number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS position FROM (SELECT tail, COUNT(*) as number FROM edges GROUP BY tail ORDER BY tail);"
      );
      int position = 0;
      while (rs.next()) {
        indexOutput.writeInt(rs.getInt("tail"));
        indexOutput.writeInt(position);
        position = rs.getInt("position");
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
