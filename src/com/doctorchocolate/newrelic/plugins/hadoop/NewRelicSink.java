package com.doctorchocolate.newrelic.plugins.hadoop;

/**
 * @author Seth Schwartzman
 */

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.metrics2.Metric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;
// import org.apache.hadoop.metrics2.MetricsException;

import com.newrelic.metrics.publish.binding.ComponentData;
import com.newrelic.metrics.publish.binding.Context;
import com.newrelic.metrics.publish.binding.Request;

public class NewRelicSink implements MetricsSink {

	private String hadoopProcType = "";
	private boolean debugEnabled = false, getGroupings = false;
	private char div = NewRelicMetrics.kMetricTreeDivider;
	private String deltaName = NewRelicMetrics.kDeltaMetricName;
	private String categoryName = NewRelicMetrics.kCategoryMetricName;
	private String nrLicenseKey;
	private Logger logger;
	private Context context;
	private ComponentData component;
	private HashMap<Integer, String[]> metricBaseNames;
	private HashMap<Integer, String> metricNames;
	private HashMap<Integer, Float> metricValues;
	private HashMap<String, Integer> metricGroupings;

	@Override
	public void init(SubsetConfiguration conf) {

		logger = Context.getLogger();

		if (conf.containsKey("proctype")) {
			hadoopProcType = conf.getString("proctype");	 
		} else {
			logger.info("Monitoring disabled for this procees.");
			logger.info("Shutting down New Relic sink.");
			return;
		}

		nrLicenseKey = conf.getString("nrlicensekey");  
		if ( "".equals(nrLicenseKey) || (nrLicenseKey == null)) {
			logger.info("No New Relic License Key given.");
			logger.info("Shutting down New Relic sink.");
			return;
		}

		context = buildContext(nrLicenseKey, conf.getString("hostname", ""), hadoopProcType);

		if (conf.getString("debug", "false").equals("true")) {
			debugEnabled = true;
			logger.info("New Relic Sink: DEBUG enabled.");
		}

		component = context.getComponents().next();

		metricNames = new HashMap<Integer, String>();
		metricBaseNames = new HashMap<Integer, String[]>();
		metricValues = new HashMap<Integer, Float>();

		if (conf.getString("nrgroupings", "false").equals("true")) {
			getGroupings = true;
			metricGroupings = new HashMap<String, Integer>();
			logger.info("New Relic Sink: Getting Metric Groupings");
		}
	}

	@Override
	public void putMetrics(MetricsRecord record) {

		Request request = new Request(context, NewRelicMetrics.kMetricInterval);
		String[] metricBases;

		if(metricBaseNames.containsKey(record.tags().hashCode()))
			metricBases = metricBaseNames.get(record.tags().hashCode());	
		else {
			String metricBase = getMetricBaseName(record, categoryName);
			String metricDeltaBase = getMetricBaseName(record, categoryName + div + deltaName);
			String metricNameTags = "", thisHostName = "", thisPort = "";

			for (MetricsTag thisTag : record.tags()) {
				if(NewRelicMetrics.HadoopTags.containsKey(thisTag.name())) {
					switch ((Integer)NewRelicMetrics.HadoopTags.get(thisTag.name())) {
					case 0:
						break;
					case 1:
						thisHostName = thisTag.value();
						break;
					case 2:
						thisPort = thisTag.value();
						break;
					default:
						break;           
					}
				} else {
					metricNameTags = metricNameTags + div + thisTag.value();
				}
			}

			if(!thisPort.isEmpty()) {
				metricBase = metricBase + div + thisPort;
				metricDeltaBase = metricDeltaBase + div + thisPort;
			} 
			if(!thisHostName.isEmpty()) {
				metricBase = metricBase + div + thisHostName;
				metricDeltaBase = metricDeltaBase + div + thisHostName;
			}
			if (!metricNameTags.isEmpty()) {
				metricBase = metricBase + div + metricNameTags;
				metricDeltaBase = metricDeltaBase + div + metricNameTags;
			}

			metricBases = new String[]{metricBase, metricDeltaBase};
			metricBaseNames.put(record.tags().hashCode(), metricBases);
		} 

		for (Metric thisMetric : record.metrics()) {

			if((thisMetric.value() == null) || (thisMetric.name() == null) || 
					thisMetric.name().isEmpty() || thisMetric.value().toString().isEmpty()) {
				// (Temporarily not) skipping "imax" and "imin" metrics,  which are constant and too large to chart
				// || thisMetric.name().contains("_imin_") || thisMetric.name().contains("_imax_")) {
				continue;
			}

			String metricName, metricType = null;
			float metricValue = thisMetric.value().floatValue();

			if(metricNames.containsKey(thisMetric.hashCode())) {
				metricName = metricNames.get(thisMetric.hashCode());
			} else {
				if((thisMetric.description() == null) || ("").equals(thisMetric.description().trim()))
					metricName = div + thisMetric.name();                
				else if (thisMetric.description().trim().endsWith("for")) {
					int metricNameCropper = thisMetric.name().indexOf('_');
					if (metricNameCropper > 0)
						metricName = div + thisMetric.description().trim() + " " + thisMetric.name().substring(0, metricNameCropper);
					else
						metricName = div + thisMetric.name();
				} else
					metricName = div + thisMetric.description();

				if (NewRelicMetrics.HadoopMetrics.containsKey(thisMetric.name())) {
					metricType = NewRelicMetrics.HadoopMetrics.get(thisMetric.name());
					if (metricType.equals("bytes")) {
						if(thisMetric.name().endsWith("GB"))
							metricValue = metricValue * NewRelicMetrics.kGigabytesToBytes;
						else if (thisMetric.name().endsWith("M"))
							metricValue = metricValue * NewRelicMetrics.kMegabytesToBytes;   
					}    
				} else if (!NewRelicMetrics.kDefaultMetricType.isEmpty())
					metricType = NewRelicMetrics.kDefaultMetricType;
				else
					metricType = "ms";

				metricName = metricName + "[" + metricType + "]";
				metricNames.put(thisMetric.hashCode(), metricName);

				if (debugEnabled && getGroupings) {
					metricGrouper(getMetricBaseName(record, categoryName), metricType);
					metricGrouper(getMetricBaseName(record, categoryName + div + deltaName), metricType);
				}
			}			

			addMetric(request, metricBases[0] + metricName, thisMetric.name(), metricValue);

			// If exists, use old metric value from hashmap for Delta, then replace with new one.
			if(metricValues.containsKey(thisMetric.hashCode()))
				addMetric(request, metricBases[1] + metricName, thisMetric.name(), Math.abs(metricValue - metricValues.get(thisMetric.hashCode())));
			metricValues.put(thisMetric.hashCode(), metricValue);
		}

		if(debugEnabled) {
			logger.info("Debug is enabled on New Relic Hadoop Extension. Metrics will not be sent.");
			if(getGroupings) {
				logger.info("Outputting metric groupings from the current Metrics Record.");
				for (Map.Entry<String, Integer> grouping : metricGroupings.entrySet()) { logger.info(grouping.getKey() + " : " + grouping.getValue()); }
			}
		} else {
			request.send();
		}
	}

	@Override
	public void flush() {}

	public static Context buildContext(String licenseKey, String hostname, String proctype) {
		Context context = new Context();
		context.licenseKey = licenseKey;
		if (hostname.isEmpty()) {
			try {
				hostname = java.net.InetAddress.getLocalHost().getHostName();
			} catch (UnknownHostException e) {
				hostname = "hadoop";
			}
		}
		context.agentData.host = hostname;
		context.agentData.version = NewRelicMetrics.kHadoopAgentVersion;
		ComponentData component = context.createComponent();
		component.guid = NewRelicMetrics.kHadoopAgentGuid;
		component.name = hostname + " " + proctype;
		return context;
	} 

	public void metricGrouper(String metricGroupingName, String metricGroupingType) {
		if ((metricGroupingName != null) && (metricGroupingType != null)) {
			String metricGrouping = metricGroupingName + "/*[" + metricGroupingType + "]";
			if(metricGroupings.containsKey(metricGrouping))
				metricGroupings.put(metricGrouping, metricGroupings.get(metricGrouping) + 1);
			else
				metricGroupings.put(metricGrouping, 1);
		}
	}

	public String getMetricBaseName(MetricsRecord thisRecord, String metricPrefix) {
		String metricGroupingName = metricPrefix + div + thisRecord.context();
		if (!thisRecord.context().toLowerCase().equals(thisRecord.name().toLowerCase()))
			metricGroupingName = metricGroupingName + div + thisRecord.name();
		return metricGroupingName;
	} 

	public void addMetric(Request request, String metricName, String metricOrigName, Float metricValue) {
		if(debugEnabled) {
			logger.info(metricName + ", " + metricOrigName + ", " + metricValue);
		} else {
			request.addMetric(component, metricName, metricValue);
		}
	}
}


