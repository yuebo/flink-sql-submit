package com.eappcat.flink.sql;

/**
 * UserOptions
 */
public class UserOptions {
    private final String sqlFilePath;
    private Source source;

    public UserOptions(String sqlFilePath) {
        this.sqlFilePath = sqlFilePath;
        assert sqlFilePath != null;
        if (sqlFilePath.toLowerCase().startsWith("http://")||sqlFilePath.toLowerCase().startsWith("https")){
            this.source=Source.URL;
        }else if(sqlFilePath.toLowerCase().startsWith("ftp://")){
            this.source=Source.FTP;
        }else {
            this.source=Source.FILE;
        }
    }

    public String getSqlFilePath() {
        return sqlFilePath;
    }

    public Source getSource() {
        return source;
    }

}
