package com.poc.CanonicalIngestionEngine.testutil;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class DbUtils {

    private static final String URL =
            "jdbc:oracle:thin:@localhost:1521/FREEPDB1";

    private static final String USER =
            "SEND_TXN_OWNER";

    private static final String PASSWORD =
            "1234";

    // =====================================================
    // GENERIC METHOD
    // =====================================================

    private Map<String, Object> executeQuery(
            String query,
            String tranId) throws Exception {

        Connection conn =
                DriverManager.getConnection(
                        URL,
                        USER,
                        PASSWORD
                );

        PreparedStatement stmt =
                conn.prepareStatement(query);

        stmt.setString(1, tranId);

        ResultSet rs =
                stmt.executeQuery();

        Map<String, Object> result =
                new HashMap<>();

        if (rs.next()) {

            ResultSetMetaData metaData =
                    rs.getMetaData();

            int columnCount =
                    metaData.getColumnCount();

            for (int i = 1; i <= columnCount; i++) {

                String columnName =
                        metaData.getColumnName(i);

                Object value =
                        rs.getObject(i);

                // =====================================
                // HANDLE NULL
                // =====================================

                if (value == null) {

                    result.put(columnName, null);
                }

                // =====================================
                // HANDLE CLOB
                // =====================================

                else if (value instanceof Clob clob) {

                    result.put(
                            columnName,
                            clob.getSubString(
                                    1,
                                    (int) clob.length()
                            )
                    );
                }

                // =====================================
                // HANDLE TIMESTAMP
                // =====================================

                else if (value instanceof Timestamp ts) {

                    result.put(
                            columnName,
                            ts.toString()
                    );
                }

                // =====================================
                // HANDLE DATE
                // =====================================

                else if (value instanceof Date dt) {

                    result.put(
                            columnName,
                            dt.toString()
                    );
                }

                // =====================================
                // HANDLE NUMBER
                // =====================================

                else if (value instanceof Number num) {

                    result.put(
                            columnName,
                            num.toString()
                    );
                }

                // =====================================
                // DEFAULT
                // =====================================

                else {

                    result.put(
                            columnName,
                            value.toString()
                    );
                }
            }
        }

        rs.close();
        stmt.close();
        conn.close();

        return result;
    }

    // =====================================================
    // SEND_TRANSACTIONS
    // =====================================================

    public Map<String, Object> getTransaction(
            String tranId) throws Exception {

        return executeQuery(
                "SELECT * FROM SEND_TRANSACTIONS WHERE TRAN_ID = ?",
                tranId
        );
    }

    // =====================================================
    // SEND_TRAN_DTL
    // =====================================================

    public Map<String, Object> getTransactionDetails(
            String tranId) throws Exception {

        return executeQuery(
                "SELECT * FROM SEND_TRAN_DTL WHERE TRAN_ID = ?",
                tranId
        );
    }

    // =====================================================
    // SEND_RECIP_DTL
    // =====================================================

    public Map<String, Object> getRecipientDetails(
            String tranId) throws Exception {

        return executeQuery(
                "SELECT * FROM SEND_RECIP_DTL WHERE TRAN_ID = ?",
                tranId
        );
    }

    // =====================================================
    // SEND_TRAN_ADDR_DTL
    // =====================================================

    public Map<String, Object> getAddressDetails(
            String tranId) throws Exception {

        return executeQuery(
                "SELECT * FROM SEND_TRAN_ADDR_DTL WHERE TRAN_ID = ?",
                tranId
        );
    }
}