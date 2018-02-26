package com.polycom.analytic.common;

import com.datatorrent.contrib.enrich.MapEnricher;

/*
 * this NoCacheMapEnricher is used to find necessary info from backend loader every time
 * without cache, it requires backend loader with high throughout  
 * */
public class NoCacheMapEnricher extends MapEnricher
{

}
