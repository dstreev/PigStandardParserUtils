package com.hortonworks.pso.udf.pig.logs.microsoft;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
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
    private BagFactory bagFactory = BagFactory.getInstance();

    private SAXBuilder builder = new SAXBuilder();
    private PathExpressionWrapper[] xpaths;

    public static enum PATHS {
        EventLog(DataType.CHARARRAY),
        RecordNumber(DataType.CHARARRAY),
        TimeGenerated(DataType.CHARARRAY),
        EventID(DataType.CHARARRAY),
        ComputerName(DataType.CHARARRAY),
        EventType(DataType.CHARARRAY),
        SourceName(DataType.CHARARRAY),
        EventCategory(DataType.CHARARRAY),
        EventTypeName(DataType.CHARARRAY),
        EventCategoryName(DataType.CHARARRAY),
        Strings(DataType.BAG),
        Message(DataType.CHARARRAY);

        private byte dataType;

        public byte getDataType() {
            return dataType;
        }

        private PATHS(byte dataType) {
            this.dataType = dataType;
        }


    }

    private class PathExpressionWrapper {
        public XPathExpression<Element> xPathExpression;
        public PATHS paths;

        private PathExpressionWrapper(XPathExpression xPathExpression, PATHS paths) {
            this.xPathExpression = xPathExpression;
            this.paths = paths;
        }

    }

    public EXTRACT_XML_LOG_EVENT() {
        try {
            xpaths = createXpaths();

        } catch (JDOMException e) {
            e.printStackTrace();
            throw new RuntimeException("Unable to create UDF", e);
        }
    }

    public PathExpressionWrapper[] createXpaths() throws JDOMException {
        PathExpressionWrapper[] paths = new PathExpressionWrapper[PATHS.values().length];
//            XPathExpression<Element>[] paths = new XPathExpression[PATHS.values().length];
        int idx = 0;
        for (PATHS path : PATHS.values()) {
            paths[idx++] =
                    new PathExpressionWrapper(XPathFactory.instance().compile("//" + path.toString(), Filters.element()), path);
        }
        return paths;
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
        for (PathExpressionWrapper pathWrapper : xpaths) {
            XPathExpression<Element> xpath = pathWrapper.xPathExpression;
            switch (pathWrapper.paths.dataType) {
                case DataType.BAG:
                    Element e = (Element) xpath.evaluateFirst(doc);
                    DataBag lclBag = bagFactory.newDefaultBag();
                    String content = e.getTextNormalize().trim();
                    String[] parts = content.split("\\|");
                    Tuple partTuple = tupleFactory.newTuple(parts.length);
                    int bagTupleIdx = 0;
                    for (String part: parts) {
                        partTuple.set(bagTupleIdx++, part);
                    }
                    lclBag.add(partTuple);
                    ret.set(idx++, lclBag);
                    break;
                case DataType.CHARARRAY:
                    Element ec = (Element) xpath.evaluateFirst(doc);
                    ret.set(idx++, ec.getTextNormalize().trim());
                    break;
                default:
                    // TODO: More handling needed here.
                    Element ed = (Element) xpath.evaluateFirst(doc);
                    ret.set(idx++, ed.getTextNormalize().trim());
                    break;
            }
        }
        return ret;
    }

    public Schema outputSchema(Schema input) {
        try {
            Schema tupleSchema = new Schema();
            for (PATHS path : PATHS.values()) {
                tupleSchema.add(new Schema.FieldSchema(path.toString().toLowerCase(), path.getDataType()));
            }

            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
                    tupleSchema, DataType.TUPLE));
        } catch (Exception e) {
            return null;
        }
    }
}
