package com.wavefront.common;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;

import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Sampling;
import com.yammer.metrics.core.Summarizable;
import com.yammer.metrics.core.VirtualMachineMetrics;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public abstract class MetricsToTimeseries {

  public static Map<String, Double> explodeSummarizable(Summarizable metric) {
    return ImmutableMap.<String, Double>builder()
        .put("min", metric.min())
        .put("max", metric.max())
        .put("mean", metric.mean())
        .put("sum", metric.sum())
        .put("stddev", metric.stdDev())
        .build();
  }

  public static Map<String, Double> explodeSampling(Sampling sampling) {
    return ImmutableMap.<String, Double>builder()
        .put("median", sampling.getSnapshot().getMedian())
        .put("p75", sampling.getSnapshot().get75thPercentile())
        .put("p95", sampling.getSnapshot().get95thPercentile())
        .put("p99", sampling.getSnapshot().get99thPercentile())
        .put("p999", sampling.getSnapshot().get999thPercentile())
        .build();
  }

  public static Map<String, Double> explodeMetered(Metered metered) {
    return ImmutableMap.<String, Double>builder()
        .put("count", new Long(metered.count()).doubleValue())
        .put("mean", metered.meanRate())
        .put("m1", metered.oneMinuteRate())
        .put("m5", metered.fiveMinuteRate())
        .put("m15", metered.fifteenMinuteRate())
        .build();
  }

  public static Map<String, Supplier<Double>> memoryMetrics(Supplier<VirtualMachineMetrics> supplier) {
    final VirtualMachineMetrics vm = supplier.get();
    return ImmutableMap.<String, Supplier<Double>>builder()
            .put("totalInit", new Supplier<Double>() {
              @Override
              public Double get() {
                return vm.totalInit();
              }
            })
            .put("totalUsed", new Supplier<Double>() {
              @Override
              public Double get() {
                return vm.totalUsed();
              }
            })
            .put("totalMax", new Supplier<Double>() {
              @Override
              public Double get() {
                return vm.totalMax();
              }
            })
            .put("totalCommitted", new Supplier<Double>() {
              @Override
              public Double get() {
                return vm.totalCommitted();
              }
            })
            .put("heapInit", new Supplier<Double>() {
              @Override
              public Double get() {
                return vm.heapInit();
              }
            })
            .put("heapUsed", new Supplier<Double>() {
              @Override
              public Double get() {
                return vm.heapUsed();
              }
            })
            .put("heapMax", new Supplier<Double>() {
              @Override
              public Double get() {
                return vm.heapMax();
              }
            })
            .put("heapCommitted", new Supplier<Double>() {
              @Override
              public Double get() {
                return vm.heapCommitted();
              }
            })
            .put("heap_usage", new Supplier<Double>() {
              @Override
              public Double get() {
                return vm.heapUsage();
              }
            })
            .put("non_heap_usage", new Supplier<Double>() {
              @Override
              public Double get() {
                return vm.nonHeapUsage();
              }
            })
            .build();
  }

  public static Map<String, Supplier<Double>> memoryPoolsMetrics(Supplier<VirtualMachineMetrics> supplier) {
    VirtualMachineMetrics vm = supplier.get();
    ImmutableMap.Builder<String, Supplier<Double>> builder = ImmutableMap.builder();
    for (final Map.Entry<String, Double> pool : vm.memoryPoolUsage().entrySet()) {
      builder.put(pool.getKey(), new Supplier<Double>() {
        @Override
        public Double get() {
          return pool.getValue();
        }
      });
    }
    return builder.build();
  }

  public static Map<String, Supplier<Double>> buffersMetrics(Supplier<VirtualMachineMetrics.BufferPoolStats> supplier) {
    final VirtualMachineMetrics.BufferPoolStats bps = supplier.get();
    return ImmutableMap.<String, Supplier<Double>>builder()
            .put("count", new Supplier<Double>() {
              @Override
              public Double get() {
                return (double) bps.getCount();
              }
            })
            .put("memoryUsed", new Supplier<Double>() {
              @Override
              public Double get() {
                return (double) bps.getMemoryUsed();
              }
            })
            .put("totalCapacity", new Supplier<Double>() {
              @Override
              public Double get() {
                return (double) bps.getTotalCapacity();
              }
            })
            .build();
  }

  public static Map<String, Supplier<Double>> vmMetrics(Supplier<VirtualMachineMetrics> supplier) {
    final VirtualMachineMetrics vm = supplier.get();
    return ImmutableMap.<String, Supplier<Double>>builder()
            .put("daemon_thread_count", new Supplier<Double>() {
              @Override
              public Double get() {
                return (double) vm.daemonThreadCount();
              }
            })
            .put("thread_count", new Supplier<Double>() {
              @Override
              public Double get() {
                return (double) vm.threadCount();
              }
            })
            .put("uptime", new Supplier<Double>() {
              @Override
              public Double get() {
                return (double) vm.uptime();
              }
            })
            .put("fd_usage", new Supplier<Double>() {
              @Override
              public Double get() {
                return vm.fileDescriptorUsage();
              }
            })
            .build();
  }

  public static Map<String, Supplier<Double>> threadStateMetrics(Supplier<VirtualMachineMetrics> supplier) {
    VirtualMachineMetrics vm = supplier.get();
    ImmutableMap.Builder<String, Supplier<Double>> builder = ImmutableMap.builder();
    for (final Map.Entry<Thread.State, Double> entry : vm.threadStatePercentages().entrySet()) {
      builder.put(entry.getKey().toString().toLowerCase(), new Supplier<Double>() {
        @Override
        public Double get() {
          return entry.getValue();
        }
      });
    }
    return builder.build();
  }

  public static Map<String, Supplier<Double>> gcMetrics(
      Supplier<VirtualMachineMetrics.GarbageCollectorStats> supplier) {
    final VirtualMachineMetrics.GarbageCollectorStats gcs = supplier.get();
    return ImmutableMap.<String, Supplier<Double>>builder()
            .put("runs", new Supplier<Double>() {
              @Override
              public Double get() {
                return (double) gcs.getRuns();
              }
            })
            .put("time", new Supplier<Double>() {
              @Override
              public Double get() {
                return (double) gcs.getTime(TimeUnit.MILLISECONDS);
              }
            })
            .build();
  }


  private static final Pattern SIMPLE_NAMES = Pattern.compile("[^a-zA-Z0-9_.\\-~]");

  public static String sanitize(String name) {
    return SIMPLE_NAMES.matcher(name).replaceAll("_");
  }

  public static String sanitize(MetricName metricName) {
    return sanitize(metricName.getGroup() + "." + metricName.getName());
  }

}
