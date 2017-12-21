package com.polycom.analytic.benchmark;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.mvel2.MVEL;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.polycom.analytic.event.rule.MvelCacheRuleDefManager;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 6, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class MvelExprTest
{
    private static final String jsonStr = "{\"uploadTime\":\"2017-11-28T10:08:22.0003\",\"siteID\":\"7ff3-7ff8-4140-ac17-3217e96291c2\",\"roomID\":\"77f79221-3a8d-4984-a8e9-efccc7e84ea8\",\"tenantID\":\"9384b5bf-52a1-40f0-8faa-83f9d82c49fd\",\"customerID\":\"4254d035-f1f0-45b4-9a9c-013f9099235a\",\"deviceID\":\"65f32291-89f8-4140-ac17-3217e9629178\",\"macAddress\":\"00:04:F2:7B:7F:9F\",\"serialNumber\":\"0004F27B7F9F\",\"arrivalTime\":\"2017-11-15T11:49:22.659Z\",\"realIP\":\"140.242.214.5\",\"range\":[2364724736,2364724991],\"country\":\"AP\",\"region\":\"\",\"city\":\"t5\",\"ll\":[35,105],\"metro\":0,\"zip\":0,\"eventType\":\"deviceError\",\"eventTime\":\"2017-11-01T08:44:55.0003\",\"message\":\"Power insufficient\",\"severity\":\"CRITICAL\"}";
    private Map<String, Object> event;
    private static final String rule = "event.get('severity').equals('CRITICAL')";
    private volatile Boolean result;
    private Serializable compileExpression;
    private MvelCacheRuleDefManager cache;

    @Setup
    public void init()
    {
        JSONObject jsonObj = JSON.parseObject(jsonStr);
        event = jsonObj.getInnerMap();
        compileExpression = MVEL.compileExpression(rule);
        cache = new MvelCacheRuleDefManager();
        cache.setCacheSize(500);
        cache.init();
    }

    @Benchmark
    public void testInterpretMode()
    {
        Map vars = new HashMap();
        vars.put("event", event);
        result = (Boolean) MVEL.eval(rule, vars);
        // print(result);
    }

    @Benchmark
    public void testCompileMode()
    {
        Map vars = new HashMap();
        vars.put("event", event);
        result = (Boolean) MVEL.executeExpression(compileExpression, vars);
    }

    @Benchmark
    public void testCacheMode()
    {
        Map vars = new HashMap();
        vars.put("event", event);
        result = (Boolean) MVEL.executeExpression(cache.getRuleDef(rule), vars);
    }

    private void print(Boolean result)
    {

    }

    public static void main(String[] args) throws RunnerException
    {
        Options options = new OptionsBuilder().include(MvelExprTest.class.getSimpleName())
                .output("/home/plcm/benchmark.log").forks(1).build();
        new Runner(options).run();
    }
}
