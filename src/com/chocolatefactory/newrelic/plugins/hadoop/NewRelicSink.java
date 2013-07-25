package com.chocolatefactory.newrelic.plugins.hadoop;

/**
 * @author Seth Schwartzman
 */

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;

import com.newrelic.metrics.publish.binding.ComponentData;
import com.newrelic.metrics.publish.binding.Context;
import com.newrelic.metrics.publish.binding.Request;

public class NewRelicSink implements MetricsSink {

	private boolean debugEnabled, getGroupings;
	private char div;
	private String hadoopProcType, categoryName, deltaName, overviewName, nrLicenseKey;
	private Logger logger;
	private Context context;
	private ComponentData component;
	private HashMap<Integer, String> metricBaseNames;
	private HashMap<String, Integer> metricGroupings;
	private HashMap<String, String[]> metricNames;
	private HashMap<String, Float> oldMetricValues;
	private HashMap<String, Float> summaryMetrics;

	@Override
	public void init(SubsetConfiguration conf) {
		debugEnabled = false;
		getGroupings = false;
		div = NewRelicMetrics.kMetricTreeDivider;
		categoryName = NewRelicMetrics.kCategoryMetricName;
		deltaName = NewRelicMetrics.kDeltaMetricName;
		overviewName = NewRelicMetrics.kOverviewMetricName;
		
		logger = Context.getLogger();

		if (conf.containsKey("proctype"))
			hadoopProcType = conf.getString("proctype");	 
		else if (conf.containsKey("enabled"))
			hadoopProcType = NewRelicMetrics.kDefaultAgentName;
		else {
			logger.info("Monitoring disabled for this procees.");
			logger.info("Shutting down New Relic sink.");
			return;
		}

		nrLicenseKey = conf.getString("nrlicensekey", "");  
		if ( "".equals(nrLicenseKey) || (nrLicenseKey == null)) {
			logger.info("No New Relic License Key given.");
			logger.info("Shutting down New Relic sink.");
			return;
		}

		context = buildContext(nrLicenseKey, conf.getString("hostname", ""), hadoopProcType);
		component = context.getComponents().next();
		
		if (conf.getString("debug", "false").equals("true")) {
			debugEnabled = true;
			logger.info("New Relic Sink: DEBUG enabled.");
		}
		
		if (conf.getString("nrgroupings", "false").equals("true")) {
			getGroupings = true;
			metricGroupings = new HashMap<String, Integer>();
			logger.info("New Relic Sink: Getting Metric Groupings");
		}
		
		metricBaseNames = new HashMap<Integer, String>();
		metricNames = new HashMap<String, String[]>();
		oldMetricValues = new HashMap<String, Float>();
		summaryMetrics = new HashMap<String, Float>();	
	}

	@SuppressWarnings("unused")
	@Override
	public void putMetrics(MetricsRecord record) {
		Request request = new Request(context, NewRelicMetrics.kMetricInterval);
		String metricBaseName;
		int recordHashCode = record.tags().hashCode();
		
		if(metricBaseNames.containsKey(recordHashCode))
			metricBaseName = metricBaseNames.get(recordHashCode);	
		else {
			metricBaseName = getMetricBaseName(record, "");
			String metricNameTags = "", hostname = "", port = "";

			for (MetricsTag tag : record.tags()) {
				if ((tag.value() == null) || tag.value().isEmpty())
					continue;
				else if(NewRelicMetrics.HadoopTags.containsKey(tag.name())) {
					switch ((Integer)NewRelicMetrics.HadoopTags.get(tag.name())) {
					case 0:
						break;
					case 1:
						hostname = tag.value();
						break;
					case 2:
						port = tag.value();
						break;
					default:
						break;           
					}
				} else if (metricNameTags.isEmpty()) 
					metricNameTags = tag.value();
				else
					metricNameTags = metricNameTags + div + tag.value();
			}
			
	    	// Skipping hostname & port to minimize metric count. Will add back if deemed valuable.
			/* 
			if(!port.isEmpty()) {
				metricBase = metricBase + div + port;
				metricDeltaBase = metricDeltaBase + div + port;
			} 
			if(!hostname.isEmpty()) {
				metricBase = metricBase + div + hostname;
				metricDeltaBase = metricDeltaBase + div + hostname;
			} 
			*/
			
			if (!metricNameTags.isEmpty())
				metricBaseName = metricBaseName + div + metricNameTags;
			
			metricBaseNames.put(recordHashCode, metricBaseName);
		} 
		
		String[] recordMetricBases = new String[] {
				// Original metric
				categoryName + div + metricBaseName,
				// Delta Metric - grouped separately under 'delta'
				categoryName + div + deltaName + div + metricBaseName,
				// Overview Dashboard Metrics - grouped separately under 'overview' and 'overview_delta'
				categoryName + div + overviewName + div + metricBaseName,
				categoryName + div + overviewName + "_" + deltaName + div + metricBaseName,
				// Summary Metrics - universally named (no context)
				categoryName + div + overviewName
		};
		
		// When iterating through metrics, if it finds a metric to be used in summary (one of the "overview metrics"), 
		// it will set this to true and initialize the summary metrics to be aggregated.
		Boolean hasOverview = false;
				
		for (AbstractMetric metric : record.metrics()) {			
			if((metric.value() == null) || (metric.name() == null) || 
				metric.name().isEmpty() || metric.value().toString().isEmpty()) {
				// NOT skipping "imax" and "imin" metrics,  which are constant and rather large
				// || metric.name().contains("_imin_") || metric.name().contains("_imax_")) {
				continue;
			}
			
			String metricName, metricType;
			String metricHashCode = recordHashCode + "" + metric.hashCode();
			Float metricValue = adjustMetricValue(metric).floatValue();
					
			if(metricNames.containsKey(metricHashCode)) {
				metricName = metricNames.get(metricHashCode)[0];
				metricType = metricNames.get(metricHashCode)[1];
			} else {
				metricName = getMetricName(metric);
				metricType = getMetricType(metric);
				
				metricNames.put(metricHashCode, new String[]{metricName, metricType});

				if (debugEnabled && getGroupings) {
					addMetricGroup(getMetricBaseName(record, categoryName), metricType);
					addMetricGroup(getMetricBaseName(record, categoryName + div + deltaName), metricType);
				}
			}						
			
			// Initialize delta to 0
			// If old metric value exists, use it to compute delta. 
			// In any case, set oldValue to use for next delta.
			Float deltaMetricValue = (float) 0;
			Float oldMetricValue = oldMetricValues.get(metricHashCode);
			if ((oldMetricValue != null) && (metricValue > oldMetricValue))
					deltaMetricValue = metricValue - oldMetricValue;	
						
			oldMetricValues.put(metricHashCode, metricValue);
			
			addMetric(request, recordMetricBases[0] + metricName, metric.name(), metricType, metricValue);
			addMetric(request, recordMetricBases[1] + metricName, metric.name(), metricType, deltaMetricValue);
			
			if(record.name().equalsIgnoreCase(hadoopProcType) && NewRelicMetrics.HadoopOverviewMetrics.contains(metricType)) {
				// This record will fill the summary metrics, as it has overview metrics.
				// If first metric of this record to have an overview metric, initializes summary metrics.
				if (!hasOverview) {
					hasOverview = true;
					Iterator<String> initOverview = NewRelicMetrics.HadoopOverviewMetrics.iterator();
					while (initOverview.hasNext()) {
						summaryMetrics.put(initOverview.next(), (float) 0);
					}
					summaryMetrics.put(metricType, deltaMetricValue);
				} else
					summaryMetrics.put(metricType, summaryMetrics.get(metricType) + deltaMetricValue);
					
				addMetric(request, recordMetricBases[2] + metricName, metric.name(), metricType, metricValue);
				addMetric(request, recordMetricBases[3] + metricName, metric.name(), metricType, deltaMetricValue);		
			}	
		}
		
		// Get summary metrics, reset each one after output.
		for(Entry<String, Float> summaryMetric : summaryMetrics.entrySet()) {
			addMetric(request, recordMetricBases[4] + div + "total " + summaryMetric.getKey(), summaryMetric.getKey(), summaryMetric.getKey(), summaryMetric.getValue());
		}
		
		if(debugEnabled) {
			logger.info("Debug is enabled on New Relic Hadoop Extension. Metrics will not be sent.");
			if(getGroupings) {
				logger.info("Outputting metric groupings from the current Metrics Record.");
				for (Map.Entry<String, Integer> grouping : metricGroupings.entrySet()) { 
					logger.info(grouping.getKey() + " : " + grouping.getValue()); 
				}
			}
		} else
			request.send();
	}

	@Override
	public void flush() {}
	
	public String getMetricName(AbstractMetric metric) {
		
		if((metric.description() == null) || ("").equals(metric.description().trim()))
			return div + metric.name();                
		else if (metric.description().trim().endsWith("for")) {
			if (metric.name().contains("_"))
				return div + metric.description().trim() + " " + metric.name().substring(0, metric.name().indexOf('_'));
			else
				return div + metric.name();
		} else
			return div + metric.description();
	}
	
	public String getMetricType(AbstractMetric metric) {
		if (NewRelicMetrics.HadoopMetrics.containsKey(metric.name())) {
			return NewRelicMetrics.HadoopMetrics.get(metric.name());
		} else
			return NewRelicMetrics.kDefaultMetricType;
	}
	
	public Number adjustMetricValue(AbstractMetric metric) {
		if(metric.name().endsWith("GB"))
			return metric.value().floatValue() * NewRelicMetrics.kGigabytesToBytes;
		else if (metric.name().endsWith("M"))
			return metric.value().floatValue() * NewRelicMetrics.kMegabytesToBytes;   
		else 
			return metric.value();
	}
	
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

	public void addMetricGroup(String metricGroupingName, String metricGroupingType) {
		if ((metricGroupingName != null) && (metricGroupingType != null)) {
			String metricGrouping = metricGroupingName + "/*[" + metricGroupingType + "]";
			if(metricGroupings.containsKey(metricGrouping))
				metricGroupings.put(metricGrouping, metricGroupings.get(metricGrouping) + 1);
			else
				metricGroupings.put(metricGrouping, 1);
		}
	}

	public String getMetricBaseName(MetricsRecord record, String metricPrefix) {
		String metricGroupingName = "";
		if(!metricPrefix.isEmpty())
			metricGroupingName = metricPrefix + div + record.context();
		else
			metricGroupingName = record.context();
		if (!record.context().equalsIgnoreCase(record.name()) && !record.name().isEmpty())
			metricGroupingName = metricGroupingName + div + record.name();
		return metricGroupingName;
	} 

	public void addMetric(Request request, String metricName, String metricOrigName, String metricType, Number metricValue) {
		if(debugEnabled)
			logger.info(metricName + ", " + metricOrigName + ", " + metricType + ", " + metricValue);
		else
			request.addMetric(component, metricName + "[" + metricType + "]", metricValue);
	}
}


