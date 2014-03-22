package com.hortonworks.pso.udf.pig.logs.microsoft;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.filter.Filters;
import org.jdom2.input.SAXBuilder;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;

import java.io.IOException;
import java.io.StringReader;

/**
 * Using XPaths to extract fields from MS Logs (in XML representation)
 */
public class EXTRACT_XML_LOG_EVENT extends EvalFunc<Tuple> {
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    private SAXBuilder builder = new SAXBuilder();
    private XPathExpression<Element>[] xpaths;

    public static enum PATHS {
        EventLog,
        RecordNumber,
        TimeGenerated,
        EventID,
        ComputerName,
        EventType,
        SourceName,
        EventCategory,
        EventTypeName,
        EventCategoryName,
        Strings,
        Message;

        public static XPathExpression<Element>[] createXpaths() throws JDOMException {
            XPathExpression<Element>[] paths = new XPathExpression[PATHS.values().length];
            int idx = 0;
            for (PATHS path : PATHS.values()) {
                paths[idx++] =
                        XPathFactory.instance().compile("//" + path.toString(), Filters.element());
            }
            return paths;
        }

    }

    public EXTRACT_XML_LOG_EVENT() {
        try {
            xpaths = PATHS.createXpaths();

        } catch (JDOMException e) {
            e.printStackTrace();
            throw new RuntimeException("Unable to create UDF", e);
        }
    }

    @Override
    public Tuple exec(Tuple objects) throws IOException {
        Tuple ret = tupleFactory.newTuple(xpaths.length);
        String xmlSnippet = (String) objects.get(0);
        Document doc = null;
        try {
            doc = builder.build(new StringReader(xmlSnippet));
        } catch (JDOMException e) {
            throw new RuntimeException("Unable to parse XML: " + xmlSnippet, e);
        }
        int idx = 0;
        for (XPathExpression<Element> xpath : xpaths) {
            Element e = (Element) xpath.evaluateFirst(doc);
            ret.set(idx++, e.getTextNormalize().trim());

        }
        return ret;
    }

    public Schema outputSchema(Schema input) {
        try {
            Schema tupleSchema = new Schema();
            for (PATHS path : PATHS.values()) {
                tupleSchema.add(new Schema.FieldSchema(path.toString().toLowerCase(), DataType.CHARARRAY));
            }

            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
                    tupleSchema, DataType.TUPLE));
        } catch (Exception e) {
            return null;
        }
    }
}
