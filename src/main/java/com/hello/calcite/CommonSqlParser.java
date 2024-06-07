package com.hello.calcite;

import com.hello.calcite.common.EngineType;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.SourceStringReader;
import org.apache.calcite.util.Util;

import java.io.Reader;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

/**
 * 通用SQL解析器
 * <p>
 * @author LeifChen
 */
@Slf4j
public class CommonSqlParser {

    private final SqlWriterConfig SQL_WRITER_CONFIG = SqlPrettyWriter.config()
            .withAlwaysUseParentheses(true)
            .withUpdateSetListNewline(false)
            .withFromFolding(SqlWriterConfig.LineFolding.TALL)
            .withIndentation(0);

    private final List<EngineType> tryParserList = Arrays.asList(EngineType.PRESTO, EngineType.HIVE, EngineType.SPARK);

    private SqlParser getSqlParser(Reader source,
                                   UnaryOperator<SqlParser.Config> transform,
                                   EngineType engineType) {
        final SqlParser.Config config = getSqlParserConfig(transform, engineType);
        return SqlParser.create(source, config);
    }

    private SqlParser.Config getSqlParserConfig(UnaryOperator<SqlParser.Config> transform, EngineType engineType) {
        final SqlParser.Config configBuilder = SqlParser.config()
                .withParserFactory(SqlParserImpl.FACTORY)
                .withUnquotedCasing(Casing.UNCHANGED)
                .withQuotedCasing(Casing.UNCHANGED)
                .withCaseSensitive(false)
                .withConformance(getDefaultSqlConformanceEnum(engineType));
        return transform.apply(configBuilder);
    }

    /**
     * 根据用户输入的EngineType获取不同引擎的解析配置
     * @param engineType 引擎类型
     * @return 返回对应引擎的解析配置
     */
    private SqlDialect getDefaultSqlDialect(EngineType engineType) {
        switch (engineType) {
            case SPARK:
                return SparkSqlDialect.DEFAULT;
            case PRESTO:
                return PrestoSqlDialect.DEFAULT;
            case TEZ:
            case HIVE:
                SqlDialect.Context hiveContext = SqlDialect.EMPTY_CONTEXT
                        .withDatabaseProduct(SqlDialect.DatabaseProduct.HIVE)
                        .withQuotedCasing(Casing.UNCHANGED)
                        .withUnquotedCasing(Casing.UNCHANGED)
                        .withNullCollation(NullCollation.LOW)
                        .withIdentifierQuoteString("`")
                        .withCaseSensitive(false);
                return new H2SqlDialect(hiveContext);
            default:
                return CalciteSqlDialect.DEFAULT;
        }
    }

    /**
     * 根据用户输入的EngineType获取不同引擎输出SQL的配置
     * @param engineType 引擎类型
     * @return 返回对应引擎输出SQL的配置
     */
    private SqlConformanceEnum getDefaultSqlConformanceEnum(EngineType engineType) {
        switch (engineType) {
            case PRESTO:
                return SqlConformanceEnum.PRESTO;
            case HIVE:
            case TEZ:
            case SPARK:
            default:
                return SqlConformanceEnum.DEFAULT;
        }
    }

    private UnaryOperator<SqlParser.Config> getTransform(SqlDialect dialect) {
        return dialect == null ? UnaryOperator.identity() : dialect::configureParser;
    }

    private SqlNode parseStmtAndHandleEx(String sql,
                                         UnaryOperator<SqlParser.Config> transform,
                                         Consumer<SqlParser> parserChecker,
                                         EngineType engineType) throws SqlParseException {
        final Reader reader = new SourceStringReader(sql);
        final SqlParser parser = getSqlParser(reader, transform, engineType);
        final SqlNode sqlNode;
        sqlNode = parser.parseStmt();
        parserChecker.accept(parser);
        return sqlNode;
    }

    /**
     * 解析SQL为抽象语法树SqlNode
     * @param sql        SQL语句
     * @param engineType 引擎类型
     * @return 返回经过解析SQL得到抽象语法树SqlNode
     * @throws SqlParseException
     */
    public SqlNode parserSql(String sql, EngineType engineType) throws SqlParseException {
        if (EngineType.AUTO.equals(engineType)) {
            for (EngineType type : tryParserList) {
                // 逐个尝试进行解析，直到解析成功
                try {
                    final UnaryOperator<SqlParser.Config> transform = getTransform(
                            getDefaultSqlDialect(type));
                    return parseStmtAndHandleEx(sql, transform, parser -> {
                    }, engineType);
                } catch (Exception e) {
                    log.error("parser sql error", e);
                }
            }
        } else {
            final UnaryOperator<SqlParser.Config> transform = getTransform(getDefaultSqlDialect(engineType));
            return parseStmtAndHandleEx(sql, transform, parser -> {
            }, engineType);

        }
        throw new RuntimeException("parser sql can not be here");
    }

    /**
     * 实现SqlNode到指定引擎的SQL输出转换
     * @param sqlNode    SQL抽象语法树
     * @param engineType 引擎类型
     * @return 返回指定引擎的SQL输出
     */
    public String actualSql(SqlNode sqlNode, EngineType engineType) {
        SqlDialect defaultSqlDialect = getDefaultSqlDialect(engineType);
        SqlDialect first = Util.first(defaultSqlDialect, HiveSqlDialect.DEFAULT);
        SqlWriterConfig sqlWriterConfig = SQL_WRITER_CONFIG.withDialect(first);
        return sqlNode.toSqlString(c -> sqlWriterConfig).getSql();
    }

    public static void main(String[] args) throws SqlParseException {
        String sql = "select c1 from emps where `id`=1";
        CommonSqlParser commonSqlParser = new CommonSqlParser();
        SqlNode sqlNode = commonSqlParser.parserSql(sql, EngineType.HIVE);
        log.info("SqlNode:\n{}", sqlNode);
        String reWriteSql = commonSqlParser.actualSql(sqlNode, EngineType.PRESTO);
        log.info("rewrite sql:\n{}", reWriteSql);
    }
}
