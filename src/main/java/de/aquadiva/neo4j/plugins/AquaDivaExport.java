package de.aquadiva.neo4j.plugins;

import static de.julielab.neo4j.plugins.constants.semedico.ConceptConstants.PROP_PREF_NAME;
import static de.julielab.neo4j.plugins.constants.semedico.ConceptConstants.PROP_SRC_IDS;
import static de.julielab.neo4j.plugins.constants.semedico.ConceptConstants.PROP_SYNONYMS;
import static de.julielab.neo4j.plugins.constants.semedico.ConceptConstants.PROP_WRITING_VARIANTS;
import static de.julielab.neo4j.plugins.constants.semedico.NodeConstants.PROP_ID;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Name;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;

import de.julielab.neo4j.plugins.ConceptManager.EdgeTypes;
import de.julielab.neo4j.plugins.ConceptManager.TermLabel;
import de.julielab.neo4j.plugins.Export;
import de.julielab.neo4j.plugins.constants.semedico.ConceptConstants;

public class AquaDivaExport extends ServerPlugin {
	private static final Logger log = Logger.getLogger(Export.class.getName());

	/**
	 * The REST context path to this plugin. This is for convenience for usage
	 * from external programs that make use of the plugin.
	 */
	public static final String AD_EXPORT_ENDPOINT = "/db/data/ext/" + AquaDivaExport.class.getSimpleName()
			+ "/graphdb/";

	public static final String AQUADIVA_LINGPIPE_DICT = "aquadiva_lingpipe_dictionary";
	public static final String AGGR_ELEMENT_MAPPING = "aggregate_element_mapping";

	@Name(AQUADIVA_LINGPIPE_DICT)
	@Description("Creates a dictionary of all synonyms and writing variants "
			+ "for terms in the database. The dictionary has three columns, "
			+ "the synonym/writing variant, the term's ID and the term's source respective to the ID. This dictionary is "
			+ "used with the Lingpipe chunker to recognize database terms in user queries. "
			+ "The returned data is a JSON array of bytes. Those bytes represent the "
			+ "GZIPed string data of the dictionary. That is, to read the actual file "
			+ "content, the JSON array is to be converted to a byte[] which then serves "
			+ "as input for a ByteArrayInputStream which in turn goes through a "
			+ "GZIPInputStream for decoding. The result is a stream from which the " + "dictionary data can be read.")
	@PluginTarget(GraphDatabaseService.class)
	public String exportLingpipeDictionary(@Source GraphDatabaseService graphDb) throws IOException {
		Label label = Label.label("MAPPING_AGGREGATE");

		log.info("Exporting AquaDiva ontology selection lingpipe dictionary data for nodes with label \"" + label.name()
				+ "\".");
		ByteArrayOutputStream baos = new ByteArrayOutputStream(Export.OUTPUTSTREAM_INIT_SIZE);
		try (GZIPOutputStream os = new GZIPOutputStream(baos)) {
			try (Transaction tx = graphDb.beginTx()) {
				ResourceIterator<Node> terms = graphDb.findNodes(label);
				int count = 0;
				while (terms.hasNext()) {
					Node term = (Node) terms.next();
					count++;

					if (term.hasProperty(PROP_ID) && term.hasProperty(PROP_PREF_NAME)) {
						Set<String> categoryStrings = new HashSet<>();

						if (term.hasLabel(TermLabel.AGGREGATE)) {
							String id = (String) term.getProperty(PROP_ID);
							// Iterable<Relationship> eleRels = term
							// .getRelationships(Direction.OUTGOING,
							// EdgeTypes.HAS_ELEMENT);
							// for (Relationship eleRel : eleRels) {
							// Node element = eleRel.getEndNode();
							// String[] sources = (String[]) element
							// .getProperty(PROP_SOURCES);
							// for (int i = 0; i < sources.length; i++) {
							// String source = sources[i];
							// categoryStrings.add(id + "||" + source);
							// }
							// }
							categoryStrings.add(id);
						} else {
							// this term is no aggregate
							Set<String> srcIdSet = new HashSet<>();
							String[] sourceIds = (String[]) term.getProperty(PROP_SRC_IDS);
							// String[] sources = (String[]) term
							// .getProperty(PROP_SOURCES);
							// if (sourceIds.length != sources.length)
							// throw new IllegalArgumentException(
							// "The properties \""
							// + PROP_SRC_IDS
							// + "\" and \""
							// + PROP_SOURCES
							// + "\" on term "
							// + PropertyUtilities
							// .getNodePropertiesAsString(term)
							// + " do not have all the same number of value
							// elements which is required for dictionary
							// creation by this method.");
							// for (int i = 0; i < sources.length; i++)
							// categoryStrings.add(sourceIds[i] + "||"
							// + sources[i]);

							for (int i = 0; i < sourceIds.length; i++)
								categoryStrings.add(sourceIds[i]);

							for (int i = 0; i < sourceIds.length; i++)
								srcIdSet.add(sourceIds[i]);

							for (String srcId : srcIdSet)
								writeNormalizedDictionaryEntry(srcId, srcId, os);
						}

						for (String categoryString : categoryStrings) {
							String preferredName = (String) term.getProperty(PROP_PREF_NAME);
							String[] synonyms = new String[0];
							if (term.hasProperty(PROP_SYNONYMS))
								synonyms = (String[]) term.getProperty(PROP_SYNONYMS);
							String[] writingVariants = new String[0];
							if (term.hasProperty(PROP_WRITING_VARIANTS))
								writingVariants = (String[]) term.getProperty(PROP_WRITING_VARIANTS);

							writeNormalizedDictionaryEntry(preferredName, categoryString, os);
							for (String synonString : synonyms)
								writeNormalizedDictionaryEntry(synonString, categoryString, os);
							for (String variant : writingVariants)
								writeNormalizedDictionaryEntry(variant, categoryString, os);
						}
					}
					if (count % 100000 == 0)
						log.info(count + " terms processed.");
				}
			}
		}
		log.info("Done exporting Lingpipe term dictionary.");
		byte[] bytes = baos.toByteArray();
		String encoded = DatatypeConverter.printBase64Binary(bytes);
		return encoded;
	}

	private void writeNormalizedDictionaryEntry(String name, String termId, OutputStream os) throws IOException {
		String normalizedName = StringUtils.normalizeSpace(name);
		if (normalizedName.length() > 2)
			IOUtils.write(normalizedName + "\t" + termId + "\n", os, "UTF-8");
	}

	@Name(AGGR_ELEMENT_MAPPING)
	@Description("Creates a GZIPed and base64-encoded ID mapping file from aggregate IDs to the IDs of their elements. For the elements, the ID property may be specified. For the aggregates, the default ID property is used.")
	@PluginTarget(GraphDatabaseService.class)
	public String exportAggregateElementMapping(@Source GraphDatabaseService graphDb) throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(Export.OUTPUTSTREAM_INIT_SIZE);
		try (GZIPOutputStream os = new GZIPOutputStream(baos)) {
			try (Transaction tx = graphDb.beginTx()) {
				ResourceIterator<Node> aggregates = graphDb.findNodes(TermLabel.AGGREGATE);
				while (aggregates.hasNext()) {
					Node agg = (Node) aggregates.next();
					IOUtils.write(agg.getProperty(PROP_ID) + "\t", os, "UTF-8");
					Iterable<Relationship> elementRels = agg.getRelationships(Direction.OUTGOING,
							EdgeTypes.HAS_ELEMENT);
					Set<String> elementIds = new HashSet<>();
					for (Relationship elementRel : elementRels) {
						Node element = elementRel.getEndNode();
						String[] sourceIds = (String[]) element.getProperty(ConceptConstants.PROP_SRC_IDS);
						for (int i = 0; i < sourceIds.length; i++) {
							String srcId = sourceIds[i];
							elementIds.add(srcId);
						}
					}
					IOUtils.write(StringUtils.join(elementIds, "||") + "\n", os, "UTF-8");
				}
			}
		}
		byte[] bytes = baos.toByteArray();
		String encoded = DatatypeConverter.printBase64Binary(bytes);
		return encoded;
	}
}
