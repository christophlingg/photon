package de.komoot.photon.importer.elasticsearch;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import de.komoot.photon.importer.Tags;
import org.elasticsearch.search.SearchHit;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * date: 02.07.14
 *
 * @author christoph
 */
public class Converter {
	private static Joiner spaceJoiner = Joiner.on(" ").skipNulls();

	public static List<JSONObject> convert(SearchHit[] hits, final String lang, final Integer srid) {
		return Lists.transform(Arrays.asList(hits), new Function<SearchHit, JSONObject>() {
			@Nullable
			@Override
			public JSONObject apply(@Nullable SearchHit hit) {
				final Map<String, Object> source = hit.getSource();

				final JSONObject result = new JSONObject();

				if(source.containsKey("category")) {
					result.put("category", source.get("category"));
				}

				if(source.containsKey("kmt_id")) {
					final Integer kmt_id = (Integer) source.get("kmt_id");
					result.put("poi_id", toStringId(kmt_id.longValue()));
				}

				result.put("name", buildCaption(source, lang));
				result.put("point", buildPoint((Map<String, Double>) source.get("coordinate"), srid));
				buildExtent(result, source.get("extent"));
				result.put("addressEntry", buildAddressEntry(source, lang));

				return result;
			}
		});
	}

	private static void buildExtent(JSONObject result, Object extent) {
		if(extent == null) return;

		try {
			Map<String, Object> map = (Map<String, Object>) extent;
			List<List> coordinates = (List<List>) map.get("coordinates");

			final List<Double> coord1 = coordinates.get(0);
			final List<Double> coord2 = coordinates.get(1);

			final JSONObject sw = new JSONObject().put("x", Math.min(coord1.get(0), coord2.get(0))).put("y", Math.min(coord1.get(1), coord2.get(1)));
			final JSONObject ne = new JSONObject().put("x", Math.max(coord1.get(0), coord2.get(0))).put("y", Math.max(coord1.get(1), coord2.get(1)));

			result.put("extent", new JSONArray().put(sw).put(ne));
		} catch(Exception e) {
		}
	}

	private static JSONObject buildAddressEntry(Map<String, Object> source, String lang) {
		final JSONObject entry = new JSONObject();

		if(source.containsKey("housenumber")) {
			entry.put("housenumber", source.get("housenumber"));
		}

		if(source.containsKey("postcode")) {
			entry.put("zipcode", source.get("postcode"));
		}

		for(String key : new String[]{"street", "city", "country", "state"}) {
			if(source.containsKey(key))
				entry.put(key, getLocalised(source, key, lang));
		}

		return entry;
	}

	private static String buildCaption(Map<String, Object> source, String lang) {
		String name = getLocalised(source, "name", lang);
		String city = getLocalised(source, "city", lang);

		String caption;

		if(hasText(name)) {
			caption = name;
		} else {
			String street = getLocalised(source, "street", lang);
			String housenumber = (String) source.get("housenumber");

			if("de".equals(lang)) {
				caption = spaceJoiner.join(street, housenumber);
			} else {
				caption = spaceJoiner.join(housenumber, street);
			}
		}

		if(hasText(city) && !city.equals(name)) {
			caption += ", " + city;
		}

		return caption;
	}

	private static boolean hasText(String name) {
		return name != null && name.length() > 0;
	}

	private static JSONObject buildPoint(Map<String, Double> coordinate, Integer srid) {
		JSONObject point = new JSONObject();
		point.put("x", coordinate.get(Tags.KEY_LON));
		point.put("y", coordinate.get(Tags.KEY_LAT));
		return point;
	}

	private static String getLocalised(Map<String, Object> source, String fieldName, String lang) {
		final Map<String, String> map = (Map<String, String>) source.get(fieldName);
		if(map == null) return null;

		if(map.get(lang) != null) {
			return map.get(lang);
		}

		return map.get("default");
	}

	/**
	 * All possible chars for representing a number as a String. Taken from the
	 * SUN jdk sources
	 */
	private static final char[] DIGITS_62 = {'0', '1', '2', '3', '4', '5',
			'6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
			'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
			'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I',
			'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V',
			'W', 'X', 'Y', 'Z'};

	/**
	 * Returns a string representation of the first argument in the radix 62 (10
	 * DIGITS + 2X26 CHARACTERS).
	 * <p/>
	 * Example: a long value like 1000 is converted to a string like
	 * &quot;x2E&quot; (not really)
	 *
	 * @param input a <code>long</code>to be converted to a string.
	 * @return a string representation of the argument in the radix 62.
	 */
	private static String toStringId(long input) {
		char[] buf = new char[65];
		int charPos = 64;
		int radix = DIGITS_62.length;

		boolean negative = (input < 0);

		if(!negative) {
			input = -input;
		}

		while(input <= -radix) {
			buf[charPos--] = DIGITS_62[(int) (-(input % radix))];
			input = input / radix;
		}
		buf[charPos] = DIGITS_62[(int) (-input)];

		if(negative) {
			buf[--charPos] = '-';
		}

		return new String(buf, charPos, (65 - charPos));
	}
}
