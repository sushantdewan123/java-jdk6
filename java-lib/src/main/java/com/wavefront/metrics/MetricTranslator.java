package com.wavefront.metrics;

import com.google.common.base.Function;

import com.wavefront.common.Pair;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public interface MetricTranslator extends Function<Pair<MetricName, Metric>, Pair<MetricName, Metric>> {
}
