package com.spbsu.flamestream.example.graph_search;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

public enum BlobifyEdgesSqlite {
  ;

  public static void main(String[] args) throws Exception {
    try (
            final Connection inputConnection = DriverManager.getConnection("jdbc:sqlite:twitter.sqlite");
            final Connection outputConnection = DriverManager.getConnection("jdbc:sqlite:twitter_blob.sqlite");
    ) {
      final Statement statement = inputConnection.createStatement();
      final ResultSet rs = statement.executeQuery("SELECT * FROM edges ORDER BY tail;");
      outputConnection.setAutoCommit(false);
      outputConnection.createStatement().execute("CREATE TABLE tail_edges (tail integer4 PRIMARY KEY NOT NULL, heads BLOB NOT NULL) WITHOUT ROWID;");
      final PreparedStatement outputStatement =
              outputConnection.prepareStatement("INSERT INTO tail_edges (tail, heads) VALUES (?, ?)");
      if (rs.next()) {
        int headsTail = rs.getInt("tail");
        final ByteArrayOutputStream headsBytes = new ByteArrayOutputStream();
        final DataOutputStream heads = new DataOutputStream(headsBytes);
        heads.writeInt(rs.getInt("head"));
        while (rs.next()) {
          final int tail = rs.getInt("tail");
          if (headsTail != tail) {
            heads.close();
            outputStatement.setInt(1, headsTail);
            outputStatement.setBytes(2, headsBytes.toByteArray());
            outputStatement.execute();
            headsTail = tail;
            headsBytes.reset();
          }
          heads.writeInt(rs.getInt("head"));
        }
        outputStatement.setInt(1, headsTail);
        outputStatement.setBytes(2, headsBytes.toByteArray());
        outputStatement.execute();
      }
      outputConnection.commit();
    }
  }
}
