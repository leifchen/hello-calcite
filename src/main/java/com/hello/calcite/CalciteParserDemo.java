package com.hello.calcite;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

@Slf4j
public class CalciteParserDemo {

    public static void main(String[] args) throws SqlParseException {
        // SQL语句
        String sql = "select * from emps where id=1";
        // SQL解析配置
        SqlParser.Config config = SqlParser.config()
                .withParserFactory(SqlParserImpl.FACTORY)
                .withUnquotedCasing(Casing.UNCHANGED)
                .withQuotedCasing(Casing.UNCHANGED)
                .withCaseSensitive(false)
                .withConformance(SqlConformanceEnum.MYSQL_5);
        // 创建SQL解析器
        SqlParser sqlParser = SqlParser.create(sql, config);
        // 执行SQL解析
        SqlNode sqlNode = sqlParser.parseQuery();
        log.info("SqlNode Object:\n {}", sqlNode);
        // 将SqlNode输出SQL
        SqlWriterConfig sqlWriterConfig = SqlPrettyWriter.config()
                .withAlwaysUseParentheses(true)
                .withUpdateSetListNewline(false)
                .withQuoteAllIdentifiers(false)
                .withFromFolding(SqlWriterConfig.LineFolding.TALL)
                .withIndentation(0)
                .withKeywordsLowerCase(false)
                .withDialect(OracleSqlDialect.DEFAULT);
        log.info("SqlNode String:\n {}", sqlNode.toSqlString(c -> sqlWriterConfig));
    }
}
