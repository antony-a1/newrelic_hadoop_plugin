package com.newrelic.extensions.hadoop;

/**
 * @author Seth Schwartzman
 */

import java.util.HashMap;
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
 private String debugEnabled = "false";
 private char div = NewRelicMetrics.kMetricTreeDivider;
 private String nrLicenseKey, nrHost;
 private Logger logger;
 private Context context;
 private ComponentData component;
 private HashMap<Integer, String> metricBaseNames;
 private HashMap<Integer, String> metricNames;
 
 @Override
 public void init(SubsetConfiguration conf) {
     	 
     hadoopProcType = conf.getString("proctype");
     nrHost = conf.getString("nrhost");
     nrLicenseKey = conf.getString("nrlicensekey");
     
     logger = Logger.getAnonymousLogger();
 
     if ( "".equals(hadoopProcType) ) {
         hadoopProcType = conf.getPrefix();
     }
     
     debugEnabled = conf.getString("debug", "false");
    
     if ( "".equals(nrLicenseKey) || (nrLicenseKey == null) ) {
     	System.err.println("ERROR: No New Relic License Key Given");
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
 }

 @Override
 public void putMetrics(MetricsRecord record) {
	 
	Request request = new Request(context, NewRelicMetrics.kMetricInterval);
	String metricBaseName;
	
	if(!metricBaseNames.containsKey(record.tags().hashCode())) {
		
	    metricBaseName = "Component" + div + "Hadoop" + div + hadoopProcType + div + record.context();
	    String metricNameTags = "", thisHostName = "", thisPort = "";
   
	    if (!record.context().toLowerCase().equals(record.name().toLowerCase())) {
	        metricBaseName = metricBaseName + div + record.name();
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
	}
	
    for (Metric thisMetric : record.metrics()) {
    	
        if((thisMetric.value() == null) || (thisMetric.name() == null) || 
                thisMetric.name().isEmpty() || thisMetric.value().toString().isEmpty()) {
        		// (Temporarily not) skipping "imax" and "imin" metrics,  which are constant and too large to chart
        		// || thisMetric.name().contains("_imin_") || thisMetric.name().contains("_imax_")) {
            continue;
        }
        
    	String metricName;
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
                String metricType = NewRelicMetrics.HadoopMetrics.get(thisMetric.name());
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
            }
            metricNames.put(thisMetric.hashCode(), metricName);
    	} else {
    		metricName = metricNames.get(thisMetric.hashCode());
    	}
    	
    	if(debugEnabled.equals("true")) {
            logger.info(metricBaseName + ", " + metricName + ", " 
            		+ thisMetric.name() + ", " + metricValue);
        } else {
        	request.addMetric(component, metricBaseName + metricName, metricValue);
            // NewRelic.recordMetric(metricBaseName + metricName, metricValue);                
        }
    }
    
    if(debugEnabled.equals("true")) {
    	logger.info("Debug is enabled on New Relic Hadoop Extension. Metrics will not be sent.");
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
}