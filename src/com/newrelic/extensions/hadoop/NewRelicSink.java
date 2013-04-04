package com.newrelic.extensions.hadoop;

/**
 * @author Seth Schwartzman
 */

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.logging.Logger;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.metrics2.Metric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.MetricsException;

import com.newrelic.data.in.binding.ComponentData;
import com.newrelic.data.in.binding.Context;
import com.newrelic.data.in.binding.Request;

public class NewRelicSink implements MetricsSink {
    
 private String hadoopProcType = "";
 private String debugEnabled = "false";
 private PrintWriter writer;  
 private char div = NewRelicMetrics.kMetricTreeDivider;
 private String nrLicenseKey, nrHost;
 private Logger logger;
 private Context context;
 private ComponentData component;

 @Override
 public void init(SubsetConfiguration conf) {
     	 
     hadoopProcType = conf.getString("proctype");
     nrHost = conf.getString("nrhost");
     nrLicenseKey = conf.getString("nrlicensekey");
     
     logger = Logger.getAnonymousLogger();
 
     if ( "".equals(hadoopProcType) ) {
         hadoopProcType = "Hadoop";
     }
     
     debugEnabled = conf.getString("debug", "false");
     
     if (debugEnabled.equals("true")) {
        try {
          this.writer = (hadoopProcType == null ? new PrintWriter(new BufferedOutputStream(System.out)) : new PrintWriter(new FileWriter(new File(hadoopProcType + ".nrdebug.log"), true)));
        } catch (Exception e) {
          throw new MetricsException("Error creating " + hadoopProcType + ".nrdebug.log", e);
        }
     } else {
         if ( "".equals(nrLicenseKey) ) {
         	System.err.println("ERROR: No New Relic License Key Given");
      		return;
      	 }
     }
     
     if ( "".equals(nrHost) ) {
         hadoopProcType = "staging-collector.newrelic.com/platform/v1/metrics";
     }
     
     context = buildContext(logger, nrLicenseKey, nrHost);
     component = context.getComponents().next();
 }

 @Override
 public void putMetrics(MetricsRecord record) {
    Request request = new Request(context, NewRelicMetrics.kMetricInterval);
	 
    String metricBaseName = "Component" + div + "Hadoop" + div + hadoopProcType + div + record.context();
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
    
    for (Metric thisMetric : record.metrics()) {

        try {
            
            // Skipping null metrics. 
            // Also skipping "imax" and "imin" metrics,  which are constant and too large to chart
            if((thisMetric.value() == null) || (thisMetric.name() == null) || 
                    thisMetric.name().isEmpty() || thisMetric.value().toString().isEmpty() ||
                    thisMetric.name().contains("_imin_") || thisMetric.name().contains("_imax_")) {
                continue;
            }
            
            String metricName;
            float metricValue = thisMetric.value().floatValue();
            
            if((thisMetric.description() == null) || (thisMetric.description().trim().equals(""))
                    || (record.context().equals("ugi"))) {
                metricName = div + thisMetric.name();                
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
            
            if(debugEnabled.equals("true")) {
                this.writer.print(metricBaseName + metricName);
                this.writer.print("=");
                this.writer.print(metricValue + "\n");
            } else {
            	request.addMetric(component, metricBaseName + metricName, metricValue);
               // NewRelic.recordMetric(metricBaseName + metricName, metricValue);                
            }
            
        } catch (NullPointerException e) {}
    }   
}
 
 @Override
 public void flush() {
     if (debugEnabled.equals("true")) {
         this.writer.flush();
     }
 }
 
 public static Context buildContext(Logger logger, String licenseKey, String host) {
		Context context = new Context(logger);
		context.licenseKey = licenseKey;
		context.serviceURI = "http://" + host;
		context.agentData.host = NewRelicMetrics.kHadoopAgentHost;
		context.agentData.version = NewRelicMetrics.kHadoopAgentVersion;
		
		ComponentData component = context.createComponent();
		component.guid = NewRelicMetrics.kHadoopAgentGuid;
		component.name = NewRelicMetrics.kHadoopAgentName;
		return context;
	}
}