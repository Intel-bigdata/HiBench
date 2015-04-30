package HiBench;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DataGen extends Configured implements Tool {

	public static final boolean DEBUG_MODE = true;
	
	@Override
	public int run(String[] args) throws Exception {

		DataOptions options = new DataOptions(args);
		switch (options.getType()) {
			case HIVE: {
				HiveData data = new HiveData (options);
				data.generate();
				break;
			}
			case PAGERANK: {
				PagerankData data = new PagerankData(options);
				data.generate();
				break;
			}
			case BAYES: {
				BayesData data = new BayesData(options);
				data.generate();
				break;
			}
			case NUTCH: {
				NutchData data = new NutchData(options);
				data.generate();
				break;
			}
			default:
				break;
		}
		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		int result = ToolRunner.run(new Configuration(), new DataGen(), args);
		System.exit(result);
	}
}
