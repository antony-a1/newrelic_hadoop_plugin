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

import com.newrelic.metrics.publish.binding.ComponentData;
import com.newrelic.metrics.publish.binding.Context;
import com.newrelic.metrics.publish.binding.Request;

public class NewRelicSink implements MetricsSink {

	private String hadoopProcType = "";
	private boolean debugEnabled = false, getGroupings = false;
	private char div = NewRelicMetrics.kMetricTreeDivider;
	private String deltaName = NewRelicMetrics.kDeltaMetricName;
	private String overviewName = NewRelicMetrics.kOverviewMetricName;
	private String categoryName = NewRelicMetrics.kCategoryMetricName;
	private String nrLicenseKey;
	private Logger logger;
	private Context context;
	private ComponentData component;
	private HashMap<Integer, String> metricBaseNames;
	private HashMap<Integer, String[]> metricNames;
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

		metricNames = new HashMap<Integer, String[]>();
		metricBaseNames = new HashMap<Integer, String>();
		metricValues = new HashMap<Integer, Float>();

		if (conf.getString("nrgroupings", "false").equals("true")) {
			getGroupings = true;
			metricGroupings = new HashMap<String, Integer>();
			logger.info("New Relic Sink: Getting Metric Groupings");
		}
	}

	@SuppressWarnings("unused")
	@Override
	public void putMetrics(MetricsRecord record) {

		Request request = new Request(context, NewRelicMetrics.kMetricInterval);
		String metricBaseName;

		if(metricBaseNames.containsKey(record.tags().hashCode()))
			metricBaseName = metricBaseNames.get(record.tags().hashCode());	
		else {
			metricBaseName = getMetricBaseName(record, "");
			String metricNameTags = "", thisHostName = "", thisPort = "";

			for (MetricsTag thisTag : record.tags()) {
				if ((thisTag.value() == null) || thisTag.value().isEmpty())
					continue;
				else if(NewRelicMetrics.HadoopTags.containsKey(thisTag.name())) {
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
				} else if (metricNameTags.isEmpty()) 
					metricNameTags = thisTag.value();
				else
					metricNameTags = metricNameTags + div + thisTag.value();
			}
			
	    	// Skipping hostname & port to minimize metric count. Will add back if deemed valuable.
			/* 
			if(!thisPort.isEmpty()) {
				metricBase = metricBase + div + thisPort;
				metricDeltaBase = metricDeltaBase + div + thisPort;
			} 
			if(!thisHostName.isEmpty()) {
				metricBase = metricBase + div + thisHostName;
				metricDeltaBase = metricDeltaBase + div + thisHostName;
			} 
			*/
			
			if (!metricNameTags.isEmpty())
				metricBaseName = metricBaseName + div + metricNameTags;
			
			metricBaseNames.put(record.tags().hashCode(), metricBaseName);
		} 
		
		String[] thisRecordMetricBases = new String[] {
				categoryName + div + metricBaseName,
				categoryName + div + deltaName + div + metricBaseName,
				categoryName + div + overviewName + div + metricBaseName,
				categoryName + div + overviewName + "_" + deltaName + div + metricBaseName
		};
		
		for (Metric thisMetric : record.metrics()) {

			if((thisMetric.value() == null) || (thisMetric.name() == null) || 
				thisMetric.name().isEmpty() || thisMetric.value().toString().isEmpty()) {
				// NOT skipping "imax" and "imin" metrics,  which are constant and rather large
				// || thisMetric.name().contains("_imin_") || thisMetric.name().contains("_imax_")) {
				continue;
			}

			String metricName, metricType;
			float metricValue = thisMetric.value().floatValue();

			if(metricNames.containsKey(thisMetric.hashCode())) {
				metricName = metricNames.get(thisMetric.hashCode())[0];
				metricType = metricNames.get(thisMetric.hashCode())[1];
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
				} else
					metricType = NewRelicMetrics.kDefaultMetricType;
				
				metricNames.put(thisMetric.hashCode(), new String[]{metricName, metricType});

				if (debugEnabled && getGroupings) {
					metricGrouper(getMetricBaseName(record, categoryName), metricType);
					metricGrouper(getMetricBaseName(record, categoryName + div + deltaName), metricType);
				}
			}						
			
			// If old metric value exists, use it to compute Delta, then replace with new one.
			float deltaMetricValue = 0;
			if(metricValues.containsKey(thisMetric.hashCode()))
				deltaMetricValue = Math.abs(metricValue - metricValues.get(thisMetric.hashCode()));	
			
			metricValues.put(thisMetric.hashCode(), metricValue);
			
			addMetric(request, thisRecordMetricBases[0] + metricName, thisMetric.name(), metricType, metricValue);
			addMetric(request, thisRecordMetricBases[1] + metricName, thisMetric.name(), metricType, deltaMetricValue);
						
			if(record.name().equalsIgnoreCase(hadoopProcType) && NewRelicMetrics.HadoopOverviewMetrics.contains(metricType)) {
				addMetric(request, thisRecordMetricBases[2] + metricName, thisMetric.name(), metricType, metricValue);
				addMetric(request, thisRecordMetricBases[3] + metricName, thisMetric.name(), metricType, deltaMetricValue);		
			}	
		}

		if(debugEnabled) {
			logger.info("Debug is enabled on New Relic Hadoop Extension. Metrics will not be sent.");
			if(getGroupings) {
				logger.info("Outputting metric groupings from the current Metrics Record.");
				for (Map.Entry<String, Integer> grouping : metricGroupings.entrySet()) { logger.info(grouping.getKey() + " : " + grouping.getValue()); }
			}
		} else
			request.send();
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
		String metricGroupingName = "";
		if(!metricPrefix.isEmpty())
			metricGroupingName = metricPrefix + div + thisRecord.context();
		else
			metricGroupingName = thisRecord.context();
		if (!thisRecord.context().equalsIgnoreCase(thisRecord.name()) && !thisRecord.name().isEmpty())
			metricGroupingName = metricGroupingName + div + thisRecord.name();
		return metricGroupingName;
	} 

	public void addMetric(Request request, String metricName, String metricOrigName, String metricType, Float metricValue) {
		if(debugEnabled)
			logger.info(metricName + ", " + metricOrigName + ", " + metricType + ", " + metricValue);
		else
			request.addMetric(component, metricName + "[" + metricType + "]", metricValue);
	}
}


