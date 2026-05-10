import org.junit.Assert;
import org.junit.Test;
import io.acosom.flink.assertrunner.unit.FlinkSqlTestCase;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;


public class FlinkSqlTest1 extends FlinkSqlTestCase {

    @Test
    public void testAssetToCurrency() throws ExecutionException, InterruptedException, TimeoutException {

        tEnv.executeSql("INSERT INTO INPUT_TOPIC_SOURCE_1 VALUES ('1',\n"
                + "        'Name 1',\n"
                + "        'Description 1'\n"
                + "    );");

        tEnv.executeSql("INSERT INTO INPUT_TOPIC_SOURCE_2 VALUES ('11',\n"
                + "        '1',\n"
                + "        'Description 2'\n"
                + "    );");

        final var assetResult = selectRowsWithTimeout("VIEW_EXAMPLE_2", 1);
        Assert.assertEquals(1, assetResult.size());

        final var assetViewResult = selectRowsWithTimeout("OUTPUT_TOPIC_SINK_1", 1);
        Assert.assertEquals(1, assetViewResult.size());

        final var positionResult = selectRowsWithTimeout("OUTPUT_TOPIC_SINK_2", 1);
        Assert.assertEquals(1, positionResult.size());
    }

    @Override
    public String getScriptName() {
        return "flink-sql-script-1.sql";
    }
}