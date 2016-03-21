package de.komoot.photon.importer.elasticsearch;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.vividsolutions.jts.geom.Coordinate;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * date: 24.05.14
 *
 * @author christoph
 */
public class Searcher {
	private final String queryTemplate;
	private final String queryLocationBiasTemplate;
	private final Client client;

	public Searcher(Client client) {
		this.client = client;
		try {
			final ClassLoader loader = Thread.currentThread().getContextClassLoader();
			queryTemplate = IOUtils.toString(loader.getResourceAsStream("query.json"), "UTF-8");
			queryLocationBiasTemplate = IOUtils.toString(loader.getResourceAsStream("query_location_bias.json"), "UTF-8");
		} catch(Exception e) {
			throw new RuntimeException("cannot access query templates", e);
		}
	}

	public List<SearchHit> search(String query, String lang, Double lon, Double lat, int limit, boolean matchAll) {
		final ImmutableMap.Builder<String, Object> params = ImmutableMap.<String, Object>builder()
				.put("query", StringEscapeUtils.escapeJson(query))
				.put("lang", lang)
				.put("should_match", matchAll ? "\"100%\"" : "-1");
		if(lon != null) params.put("lon", lon);
		if(lat != null) params.put("lat", lat);

		StrSubstitutor sub = new StrSubstitutor(params.build(), "${", "}");
		if(lon != null && lat != null) {
			query = sub.replace(queryLocationBiasTemplate);
		} else {
			query = sub.replace(queryTemplate);
		}

		SearchResponse response = client.prepareSearch("photon").setSearchType(SearchType.QUERY_AND_FETCH).setQuery(query).setSize(limit * 2).setTimeout(TimeValue.timeValueSeconds(3)).execute().actionGet();

		List<SearchHit> hits = deduplicate(response.getHits().getHits());
		if(hits.size() > limit) {
			hits = hits.subList(0, limit);
		}
		return hits;
	}

	private List<SearchHit> deduplicate(SearchHit[] allHits) {
		List<SearchHit> hits;
		hits = deduplicatePois(allHits);
		hits = deduplicatePlaces(hits);
		return hits;
	}

	private List<SearchHit> deduplicatePlaces(List<SearchHit> allHits) {
		ArrayList<SearchHit> hits = Lists.newArrayListWithExpectedSize(allHits.size());
		ArrayListMultimap<String, Coordinate> keys = ArrayListMultimap.create();

		outerloop:
		for(SearchHit h : allHits) {
			final Map<String, Object> source = h.getSource();
			final Integer cat = (Integer) source.get("category");

			if(cat == null || (cat != 225 && cat != 229 && cat != 228)) {
				// this is neither a city nor a town
				hits.add(h);
				continue;
			}

			String name = extractName(source);
			if(name == null) {
				// place has no name -> no way to deduplicate
				hits.add(h);
				continue;
			}

			final List<Coordinate> coordinates = keys.get(name);
			Map<String, Number> coord = (Map<String, Number>) source.get("coordinate");
			Coordinate coordinate = new Coordinate(coord.get("lon").doubleValue(), coord.get("lat").doubleValue());

			if(coordinates == null) {
				keys.put(name, coordinate);
				hits.add(h);
				continue;
			}

			for(Coordinate c : coordinates) {
				// check if there was already a city with same nearby nearby
				double dist = c.distance(coordinate);
				if(dist < 0.7) {
					// about 50 km
					continue outerloop;
				}
			}

			hits.add(h);
			keys.put(name, coordinate);
		}

		return hits;
	}

	private String extractName(Map<String, Object> source) {
		final Map<String, String> name = (Map<String, String>) source.get("name");

		if(name == null) {
			return null;
		}

		return name.get("default");
	}

	@Nonnull
	private List<SearchHit> deduplicatePois(SearchHit[] allHits) {
		ArrayList<SearchHit> hits = Lists.newArrayListWithExpectedSize(allHits.length);
		Set<String> keys = Sets.newHashSetWithExpectedSize(allHits.length);

		for(SearchHit h : allHits) {
			final Map<String, Object> source = h.getSource();
			final Object osmId = source.get("osm_id");
			final Object cat = source.get("category");

			if(osmId == null || cat == null) {
				// too few information to deduplicate, take it
				hits.add(h);
				continue;
			}

			String key = String.format("%s::%s", osmId, cat);
			if(keys.contains(key)) {
				// it's a duplicate, ignore result
				continue;
			}

			hits.add(h);
			keys.add(key);
		}

		return hits;
	}
}
