package com.spbsu.flamestream.example.graph_search;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public enum PackSqliteTailHeadOffset {
  ;

  public static void main(String[] args) throws Exception {
    try (
            final FileOutputStream indexFile = new FileOutputStream("tail_head_offset.bin");
            final BufferedOutputStream outputStream = new BufferedOutputStream(indexFile, 1 << 20);
            final DataOutputStream indexOutput = new DataOutputStream(outputStream)
    ) {
      // create a database connection
      final Connection connection = DriverManager.getConnection("jdbc:sqlite:twitter.sqlite");
      final Statement statement = connection.createStatement();
      statement.setQueryTimeout(30);  // set timeout to 30 sec.
      final ResultSet rs = statement.executeQuery(
              "SELECT tail, COUNT(*) as number FROM edges GROUP BY tail ORDER BY tail;"
      );
      long headOffset = 0;
      while (rs.next()) {
        indexOutput.writeInt(rs.getInt("tail"));
        indexOutput.writeLong(headOffset);
        headOffset += rs.getInt("number");
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
