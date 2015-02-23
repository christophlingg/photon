package de.komoot.photon.importer.elasticsearch;

import com.neovisionaries.i18n.CountryCode;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;
import de.komoot.photon.ESBaseTester;
import de.komoot.photon.importer.model.PhotonDoc;
import org.junit.*;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author Peter Karich
 */
public class DeduplicationTest extends ESBaseTester {

	@Before
	public void setUp() {
		setUpES();
		deleteAll();
	}

	private final static GeometryFactory FACTORY = new GeometryFactory(new PrecisionModel(), 4326);

	@Test
	public void testBerlin() {
		Map<String, String> nameMap = new HashMap<String, String>();
		nameMap.put("name", "berlin");

		Importer instance = new Importer(getClient());
		instance.add(new PhotonDoc(1, "way", 1, "place", "city", nameMap, null, null, null, -1, 0.7, CountryCode.DE, FACTORY.createPoint(new Coordinate(13.4385964, 52.5170365)), -1, 225));
		instance.add(new PhotonDoc(2, "way", 2, "place", "city", nameMap, null, null, null, -1, 0.7, CountryCode.DE, FACTORY.createPoint(new Coordinate(13.0883476, 52.5198535)), -1, 228));
		instance.finish();

		refresh();
		assertEquals(1, new Searcher(getClient()).search("berlin", "en", null, null, 10, true).size());
	}

	@Test
	public void testCambridge() {
		Map<String, String> nameMap = new HashMap<String, String>();
		nameMap.put("name", "cambridge");

		Importer instance = new Importer(getClient());
		instance.add(new PhotonDoc(1, "way", 1, "place", "city", nameMap, null, null, null, -1, 0.7, CountryCode.DE, FACTORY.createPoint(new Coordinate(0.124862, 52.2033051)), -1, 225));
		instance.add(new PhotonDoc(2, "way", 2, "place", "city", nameMap, null, null, null, -1, 0.7, CountryCode.DE, FACTORY.createPoint(new Coordinate(0.124862, 52.2033051)), -1, 225));
		instance.finish();

		refresh();
		assertEquals(1, new Searcher(getClient()).search("cambridge", "en", null, null, 10, true).size());
	}

	@Test
	public void testPoiSansSouci() {
		Map<String, String> nameMap = new HashMap<String, String>();
		nameMap.put("name", "Schloss Sanssouci");

		Importer instance = new Importer(getClient());
		instance.add(new PhotonDoc(1, "way", 142038726, "place", "city", nameMap, null, null, null, -1, 0.7, CountryCode.DE, FACTORY.createPoint(new Coordinate(0.124862, 52.2033051)), -1, 3));
		instance.add(new PhotonDoc(2, "way", 142038726, "place", "city", nameMap, null, null, null, -1, 0.7, CountryCode.DE, FACTORY.createPoint(new Coordinate(0.124862, 52.2033051)), -1, 3));
		instance.finish();

		refresh();
		assertEquals(1, new Searcher(getClient()).search("sanssouci", "en", null, null, 10, true).size());
	}
}
