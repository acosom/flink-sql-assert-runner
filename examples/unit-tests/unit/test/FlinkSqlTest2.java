import org.junit.Assert;
import org.junit.Test;
import io.acosom.flink.assertrunner.unit.FlinkSqlTestCase;

public class FlinkSqlTest2 extends FlinkSqlTestCase {

    @Test
    public void testAssetToCurrency() throws Exception {

        tEnv.executeSql("INSERT INTO INPUT_SOURCE VALUES ('1',\n"
                + "        'abc1',\n"
                + "        'First Message'\n"
                + "    );");

        tEnv.executeSql("INSERT INTO INPUT_SOURCE VALUES ('2',\n"
                + "        'abc2',\n"
                + "        'First Message'\n"
                + "    );");

        tEnv.executeSql("INSERT INTO INPUT_SOURCE VALUES ('3',\n"
                + "        'abc3',\n"
                + "        'First Message'\n"
                + "    );");

        var result = selectAllRowsWithTimeout("OUTPUT_SINK", 15);

        Assert.assertEquals(2, result.size());
    }

    @Override
    public String getScriptName() {
        return "flink-sql-script-2.sql";
    }
}