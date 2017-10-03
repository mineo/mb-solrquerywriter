package org.musicbrainz.search.solrwriter;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.musicbrainz.mmd2.*;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

public abstract class AbstractMBWriterTest extends SolrTestCaseJ4 implements
		MBWriterTestInterface {
	@BeforeClass
	public static void beforeClass() throws Exception {
		initCore("solrconfig.xml", "schema.xml", "mbsssss", getCorename());
	}

	public static String corename;

	public static String getCorename() {
		return corename;
	}

	/**
	 * Get the document to use in the tests
	 */
	abstract ArrayList<String> getDoc();

	/**
	 * Add a document containing doc to the current core.
	 *
	 * @param withStore  whether the _store field should be populated with data
	 * @param storeValue allows specifying the value for the _store field by
	 *                   setting it to a value different from null
	 * @throws IOException
	 */
	void addDocument(boolean withStore, String storeValue) throws IOException {
		ArrayList<String> values = new ArrayList<>(getDoc());
		if (withStore) {
			String xml;
			if (storeValue != null) {
				xml = storeValue;
			} else {
				String xmlfilepath = MBWriterTestInterface.class.getResource
						(getCorename() + ".xml").getFile();
				byte[] content = Files.readAllBytes(Paths.get(xmlfilepath));
				xml = new String(content);
			}

			values.add(0, xml);
			values.add(0, "_store");
		}

		assertU(adoc((values.toArray(new String[values.size()]))));
		assertU(commit());
	}

	void addDocument(boolean withStore) throws Exception {
		addDocument(withStore, null);
	}

	@After
	public void After() {
		clearIndex();
	}

	@Test
	/**
	 * Check that the XML document returned is the same as the one we stored
	 * in the first place.
	 */
	public void performCoreTest() throws Exception {
		addDocument(true);
		String expectedFile;
		byte[] content;
		String expected;

		String expectedFileName = String.format("%s-list.%s", getCorename(),
				getExpectedFileExtension());

		expectedFile = AbstractMBWriterTest.class.getResource
				(expectedFileName).getFile();
		content = Files.readAllBytes(Paths.get(expectedFile));
		expected = new String(content);

		String response = h.query(req("q", "*:*", "wt", getWritername()));
		compare(expected, response);
	}

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	/**
	 * Check that a useful error message is shown to the user if 'score' is
	 * not in the field list.
	 */
	public void testNoScoreException() throws Exception {
		addDocument(true);
		thrown.expectMessage(MBXMLWriter.SCORE_NOT_IN_FIELD_LIST);
		h.query(req("q", "*:*", "fl", "*", "wt", getWritername()));
	}

	@Test
	/**
	 * Check that a useful error message is shown to the user if the document
	 * doesn't have a '_store' field.
	 */
	public void testNoStoreException() throws Exception {
		addDocument(false);
		thrown.expectMessage(MBXMLWriter.NO_STORE_VALUE);
		h.query(req("q", "*:*", "fl", "score", "wt", getWritername()));
	}

	@Test
	/**
	 * Check that the expected error message is shown for documents with a
	 * '_store' field with a value that can't be
	 * unmarshalled.
	 */
	public void testInvalidStoreException() throws Exception {
		addDocument(true, "invalid");
		thrown.expectMessage(MBXMLWriter.UNMARSHALLING_STORE_FAILED +
				"invalid");
		h.query(req("q", "*:*", "fl", "score", "wt", getWritername()));
	}

	@Test
	/**
	 * Check that we only return an empty metadata list if the query contains
	 * unknown field names.
	 *
	 * In this case, we just return a list containing 0 results, like the old
	 * search server did.
	 */
	public void testUnknownFieldName() throws Exception {
		SolrQueryResponse rsp = new SolrQueryResponse();
		Writer writer = new StringWriter();
		SolrQueryRequest request = req("q", "unknownFieldname:value", "wt", getWritername());
		MBXMLWriter queryResponseWriter = (MBXMLWriter) h.getCore().getQueryResponseWriter(getWritername());
		h.getCore().execute(h.getCore().getRequestHandler("/select"), request, rsp);
		queryResponseWriter.write(writer, request, rsp);

		StringReader reader = new StringReader(writer.toString());
		Metadata unmarshalledObj = (Metadata) queryResponseWriter.unmarshaller.unmarshal(reader);
		Object MMDList = null;
		switch (queryResponseWriter.entityType) {
			case annotation:
				MMDList = unmarshalledObj.getAnnotationList();
				break;
			case area:
				MMDList = unmarshalledObj.getAreaList();
				break;
			case artist:
				MMDList = unmarshalledObj.getArtistList();
				break;
			case cdstub:
				MMDList = unmarshalledObj.getCdstubList();
				break;
			case editor:
				MMDList = unmarshalledObj.getEditorList();
				break;
			case event:
				MMDList = unmarshalledObj.getEventList();
				break;
			case instrument:
				MMDList = unmarshalledObj.getInstrumentList();
				break;
			case label:
				MMDList = unmarshalledObj.getLabelList();
				break;
			case place:
				MMDList = unmarshalledObj.getPlaceList();
				break;
			case recording:
				MMDList = unmarshalledObj.getRecordingList();
				break;
			case release:
				MMDList = unmarshalledObj.getReleaseList();
				break;
			case release_group:
				MMDList = unmarshalledObj.getReleaseGroupList();
				break;
			case series:
				MMDList = unmarshalledObj.getSeriesList();
				break;
			case tag:
				MMDList = unmarshalledObj.getTagList();
				break;
			case work:
				MMDList = unmarshalledObj.getWorkList();
				break;
			case url:
				MMDList = unmarshalledObj.getUrlList();
				break;
			default:
				// This should never happen because MBXMLWriters init method
				// aborts earlier
				throw new RuntimeException("Testing with an invalid entitytype: " + queryResponseWriter.entityType.name());
		}

		Method getCountMethod = null;
		Method getOffsetMethod = null;
		try {
			getCountMethod = MMDList.getClass().getMethod("getCount");
			getOffsetMethod = MMDList.getClass().getMethod("getOffset");
		} catch (NoSuchMethodException | SecurityException e) {
			throw new RuntimeException(e);
		}

		BigInteger count = (BigInteger) getCountMethod.invoke(MMDList);
		BigInteger offset = (BigInteger) getOffsetMethod.invoke(MMDList);

		assertEquals(count.compareTo(BigInteger.valueOf(0)), 0);
		assertEquals(offset.compareTo(BigInteger.valueOf(0)), 0);
	}
}
