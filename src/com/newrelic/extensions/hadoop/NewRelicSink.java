package com.newrelic.extensions.hadoop;

/**
 * @author Seth Schwartzman
 */

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.metrics2.Metric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;
// import org.apache.hadoop.metrics2.MetricsException;

import com.newrelic.data.in.binding.ComponentData;
import com.newrelic.data.in.binding.Context;
import com.newrelic.data.in.binding.Request;

public class NewRelicSink implements MetricsSink {
    
 private String hadoopProcType = "";
 private boolean debugEnabled = false, getGroupings = false;
 private char div = NewRelicMetrics.kMetricTreeDivider;
 private String nrLicenseKey, nrHost;
 private Logger logger;
 private Context context;
 private ComponentData component;
 private HashMap<Integer, String> metricBaseNames;
 private HashMap<Integer, String> metricNames;
 private HashMap<String, Integer> metricGroupings;
 
 @Override
 public void init(SubsetConfiguration conf) {
    
     nrHost = conf.getString("nrhost");
     nrLicenseKey = conf.getString("nrlicensekey");
     
     logger = Logger.getAnonymousLogger();
 
	 if (conf.containsKey("proctype")) {
		 hadoopProcType = conf.getString("proctype");	 
	 } else {
	     	System.out.println("Monitoring disabled for this procees.");
	     	System.out.println("Shutting down New Relic sink.");
	  		return;
	 }
     nrHost = conf.getString("nrhost");
     nrLicenseKey = conf.getString("nrlicensekey");
     if (conf.getString("debug", "false").equals("true")) {
    	 debugEnabled = true;
    	 System.out.println("New Relic Sink: DEBUG enabled.");
     }
    
     if ( "".equals(nrLicenseKey) || (nrLicenseKey == null)) {
     	System.out.println("No New Relic License Key given.");
     	System.out.println("Shutting down New Relic sink.");
  		return;
  	 }
     
     if ( "".equals(nrHost) || (nrHost == null)) {
    	 // nrHost = "staging-collector.newrelic.com/platform/v1/metrics";
    	 nrHost = Context.SERVICE_URI_HOST;
     }
     
     context = buildContext(logger, nrLicenseKey, nrHost);
     component = context.getComponents().next();
     
     metricNames = new HashMap<Integer, String>();
     metricBaseNames = new HashMap<Integer, String>();
     if (conf.getString("nrgroupings", "false").equals("true")) {
    	 getGroupings = true;
    	 metricGroupings = new HashMap<String, Integer>();
    	 System.out.println("New Relic Sink: Getting Metric Groupings");
     }
 }

 @Override
 public void putMetrics(MetricsRecord record) {
	 
	Request request = new Request(context, NewRelicMetrics.kMetricInterval);
	String metricBaseName, metricGroupingName = null;
	
	if(!metricBaseNames.containsKey(record.tags().hashCode())) {
		String metricNameTags = "", thisHostName = "", thisPort = "";
   	    metricBaseName = getMetricGroupingName(record);	    
	        
	    // Metric Grouping is based on process & context, but not broken up by tags
	    if (getGroupings) {
	    	metricGroupingName = metricBaseName;
	    }	
	    
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
	    
	    if(!thisHostName.isEmpty()) {
	            metricBaseName = metricBaseName + div + thisHostName;
	    } 
	    if(!thisPort.isEmpty()) {
	            metricBaseName = metricBaseName + div + thisPort;
	    }
	    if (!metricNameTags.isEmpty()) {
	        metricBaseName = metricBaseName + metricNameTags;
	    }
	    metricBaseNames.put(record.tags().hashCode(), metricBaseName);
	} else {
		metricBaseName = metricBaseNames.get(record.tags().hashCode());
		if (getGroupings) {
			metricGroupingName = getMetricGroupingName(record);
		}
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
    	
    	if(!metricNames.containsKey(thisMetric.hashCode())) {
    		
            if((thisMetric.description() == null) || ("").equals(thisMetric.description().trim())) {
                metricName = div + thisMetric.name();                
            } else if (thisMetric.description().trim().endsWith("for")) {
        		int metricNameCropper = thisMetric.name().indexOf('_');
        		if (metricNameCropper > 0) {
        			metricName = div + thisMetric.description().trim() + " " + thisMetric.name().substring(0, metricNameCropper);
        		} else {
        			metricName = div + thisMetric.name();
        		}
        	} else {
        		metricName = div + thisMetric.description();
        	}
            
            if (NewRelicMetrics.HadoopMetrics.containsKey(thisMetric.name())) {
                metricType = NewRelicMetrics.HadoopMetrics.get(thisMetric.name());
                metricName = metricName + "[" + metricType + "]";
                if (metricType.equals("bytes")) {
                    if(thisMetric.name().endsWith("GB")) {
                       metricValue = metricValue * NewRelicMetrics.kGigabytesToBytes;
                    } else if (thisMetric.name().endsWith("M")) {
                        metricValue = metricValue * NewRelicMetrics.kMegabytesToBytes;   
                    }
                }    
                } else if (!NewRelicMetrics.kDefaultMetricType.isEmpty()) {
                	metricName = metricName + "[" + NewRelicMetrics.kDefaultMetricType + "]";
                	metricType = NewRelicMetrics.kDefaultMetricType;
                } else {
                	metricType = "ms";
                }
            	metricNames.put(thisMetric.hashCode(), metricName);
		} else {
			metricName = metricNames.get(thisMetric.hashCode());
		}
    	
    	if(debugEnabled) {
            logger.info(metricBaseName + ", " + metricName + ", " 
            		+ thisMetric.name() + ", " + metricValue);
            if (getGroupings) {
        		metricGrouper(metricGroupingName, metricType);
        	}
        } else {
        	request.addMetric(component, metricBaseName + metricName, metricValue);
            // NewRelic.recordMetric(metricBaseName + metricName, metricValue);                
        }
    }
    
    if(debugEnabled) {
    	logger.info("Debug is enabled on New Relic Hadoop Extension. Metrics will not be sent.");
    	if(getGroupings) {
    		logger.info("Outputting metric groupings from the current Metrics Record.");
    		putGroupings(); 
    	}
    } else {
    	request.send();
    }
 }
 
 @Override
 public void flush() {}
 
 public static Context buildContext(Logger logger, String licenseKey, String host) {
	Context context = new Context(logger);
	context.licenseKey = licenseKey;
	context.serviceURI = Context.SERVICE_URI_PROTOCOL + host;
	context.agentData.host = NewRelicMetrics.kHadoopAgentHost;
	context.agentData.version = NewRelicMetrics.kHadoopAgentVersion;
	
	ComponentData component = context.createComponent();
	component.guid = NewRelicMetrics.kHadoopAgentGuid;
	component.name = NewRelicMetrics.kHadoopAgentName;
	logger.finest("NR URI: " + context.serviceURI);
	return context;
} 

 public void metricGrouper(String metricGroupingName, String metricGroupingType) {
	 if ((metricGroupingName != null) && (metricGroupingType != null)) {
	 	 String metricGrouping = "/" + metricGroupingName + "/*[" + metricGroupingType + "]";
		 if(metricGroupings.containsKey(metricGrouping)) {
			 metricGroupings.put(metricGrouping, metricGroupings.get(metricGrouping) + 1);
		 } else {
			 metricGroupings.put(metricGrouping, 1);
		 }
	 }
}
 
 public void putGroupings() {
	for (Map.Entry<String, Integer> grouping : metricGroupings.entrySet()) { 
			logger.info(grouping.getKey() + " : " + grouping.getValue()); 
	}
}
 
public String getMetricGroupingName(MetricsRecord thisRecord) {
	 String metricGroupingName = "Component" + div + "Hadoop" + div + hadoopProcType + div + thisRecord.context();
	 if (!thisRecord.context().toLowerCase().equals(thisRecord.name().toLowerCase())) {
		 metricGroupingName = metricGroupingName + div + thisRecord.name();
	 }
	 return metricGroupingName;
} }


