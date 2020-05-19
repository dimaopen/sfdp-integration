package be.fgov.sfpd.integration.documents;

import java.io.*;
import java.net.ConnectException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Predicate;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.builder.xml.Namespaces;
import org.apache.camel.cdi.ContextName;
import org.apache.camel.processor.validation.PredicateValidationException;
import org.apache.commons.io.FilenameUtils;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.xml.sax.SAXParseException;

@ContextName("centestim")
public class ImportDocumentFromAPSoftToTheseosWorkflowRouteBuilder extends RouteBuilder {

	private static final Map<String, String> TASK_TO_WORKFLOW = Collections.singletonMap("PUBLIC_RETIREMENT_ESTIMATION", "Estimation");
	private static final String AUTHORIZATION = "{{theseos.workflow.api.authorization}}";
	private static final Namespaces NS = new Namespaces("tns", "urn:document-schema");

	private static final String HAS_EMBEDDED_WORKFLOWS_EXPR = "$._embedded.workflows[?(@.length() > 0)]";
	private static final String EMBEDDED_WORKFLOWS_LINKS_PATH = "$._embedded.workflows[0]._links.self.href";
	private static final String UPLOAD_DOC_TARGET_URL = "_forms.execute[?(@._links.target.name == 'uploadDocumentWithoutTranslation')]._links.target.href";

	private static final String VALID_FILENAME_REGEX = "\\d{11}D\\d{6}T\\d{8}\\.xml";

	private static final String WORKFLOW_DETAILS_DIRECT_URI = "direct:getWorkflow";
	private static final String WORKFLOWS_FROM_THESEOS_DIRECT_URI = "direct:workflowResults";

	private static final String WORKFLOW_HREF_URI = "${header.workflow}";
	private static final String HEADER_UPLOAD_URI = "${header.upload}";

	private static final String CAMEL_DOCUMENTS_INPUT_URI = "{{route.documents.input.uri}}?fileName=${header.file}"
			+ "&readLock=changed&readLockMinAge=3s&readLockTimeout=10000&move=success";
	private static final String THESEOS_WORKFLOW_API_PARAMETERS="?niss=${header.inss}&definition=${header.type}" +
			"&search:sortField=lastUpdateTime&search:sortOrder";
	private static final String THESEOS_WORKFLOW_API_URI = "http4://{{theseos.host}}:{{theseos.port}}/" +
			"{{theseos.workflow.api.url}}";
	private static final String XSD_VALIDATION_URI = "validator:be/fgov/sfpd/integration/documents/Document.xsd";
	private static final String INPUT_URI = "{{route.documents.input.uri}}?include=.*\\.xml"
			+ "&readLock=changed&readLockMinAge=6s&readLockTimeout=10000&move=success&moveFailed=error";


	@Override
	public void configure() {

		onException(SAXParseException.class)
				.log("Failed to validate input XML file ${header.CamelFileName}")
				.process(movePdfToErrorFolder());
		onException(ConnectException.class)
				.log("Failed to connect to Theseos workflow API while processing file ${header.CamelFileName}")
				.process(movePdfToErrorFolder());
		onException(Exception.class)
				.log("Some error occurred while processing file ${header.CamelFileName}")
				.process(movePdfToErrorFolder());
		onException(PredicateValidationException.class)
				.log("Some error occurred while uploading pdf file. Original xml was: ${header.CamelFileName}")
				.process(movePdfToErrorFolder());

		from(INPUT_URI)
				.log("Processing file ${header.CamelFileName}")
				.process(validateFilename())
				.to(XSD_VALIDATION_URI)
				.process(extractXMLValues())
				.process(mapImportTaskToWorkflowType())
				.setHeader("Authorization", simple(AUTHORIZATION))
				.process(prepareForHttpGetRequest())
				.toD(THESEOS_WORKFLOW_API_URI+THESEOS_WORKFLOW_API_PARAMETERS)
				.to(WORKFLOWS_FROM_THESEOS_DIRECT_URI);

		from(WORKFLOWS_FROM_THESEOS_DIRECT_URI)
				.convertBodyTo(String.class)
				.choice()
				.when().jsonpath(HAS_EMBEDDED_WORKFLOWS_EXPR)
				// extract first workflow url into header
				.setHeader("workflow").jsonpath(EMBEDDED_WORKFLOWS_LINKS_PATH)
				.process(e -> transformUrlInHeader(e, "workflow"))
				.log("found workflow ${header.workflow}")
				.otherwise()
				.throwException(RuntimeException.class, "No workflow found")
				.end()
				.toD(WORKFLOW_HREF_URI)
				.to(WORKFLOW_DETAILS_DIRECT_URI);

		from(WORKFLOW_DETAILS_DIRECT_URI)
				.convertBodyTo(String.class)
				.setHeader("upload").jsonpath(UPLOAD_DOC_TARGET_URL, String.class)
				.choice()
				.when(cantUpload())
				.throwException(RuntimeException.class, "No uploadDocumentWithoutTranslation transition")
				.otherwise()
				.process(e -> transformUrlInHeader(e, "upload"))
				.process(e -> setTimeOutInHeader(e,"upload", "60000"))
				.log("upload document to ${header.upload}")
				.pollEnrich().simple(CAMEL_DOCUMENTS_INPUT_URI)
				.timeout(30000)
				.aggregationStrategy(this::aggregate)
				.log("Processing file ${header.CamelFileName}")
				.process(prepareForHttpPostRequest())
				.toD(HEADER_UPLOAD_URI)
				.validate(isOkResponse())
				.end();
	}


	private Processor validateFilename() {
		return (exchange) -> {

			final String filename = exchange.getIn().getHeader(Exchange.FILE_NAME, String.class);
			if (!filename.matches(VALID_FILENAME_REGEX)) {
				throw new RuntimeException("Invalid file found in input. Filename was: " + filename
						+ ". Expected file format is: " + VALID_FILENAME_REGEX);
			}
		};
	}

	private Predicate isOkResponse() {
		return header(Exchange.HTTP_RESPONSE_CODE).isEqualTo(200);
	}

	private Predicate cantUpload() {
		return header("upload").isEqualTo("[]");
	}

	private void transformUrlInHeader(Exchange exchange, String headerName) {
		String url = exchange.getIn().getHeader(headerName, String.class);
		String correctUrl = url.replace("http:", "http4:").replace("https:", "https4:");
		exchange.getIn().setHeader(headerName, correctUrl);
	}

	private void setTimeOutInHeader(Exchange exchange, String headerName,  String milisecond) {
		String url = exchange.getIn().getHeader(headerName, String.class);
		String newUrl = url + "&connectionRequestTimeout=" + milisecond +
				"&connectTimeout=" + milisecond +
				"&socketTimeout=" + milisecond;
		exchange.getIn().setHeader(headerName, newUrl);
	}

	private Processor extractXMLValues() {
		return (exchange) -> {
			final String inss = (String) NS.xpath("/tns:Document/tns:NISS", String.class).evaluate(exchange);
			final String file = (String) NS.xpath("/tns:Document/tns:FileName", String.class).evaluate(exchange);
			final String task = (String) NS.xpath("/tns:Document/tns:ImportTask", String.class).evaluate(exchange);
			final String mime = (String) NS.xpath("/tns:Document/tns:MimeType", String.class).evaluate(exchange);

			final Message in = exchange.getIn();

			in.setHeader("inss", inss);
			in.setHeader("file", file);
			in.setHeader("task", task);
			in.setHeader("mime", mime);
		};
	}

	private Processor mapImportTaskToWorkflowType() {
		return (exchange) -> {
			final String type = TASK_TO_WORKFLOW.get(exchange.getIn().getHeader("task", String.class));
			exchange.getIn().setHeader("type", type);
		};
	}

	private Processor prepareForHttpGetRequest() {
		return (exchange) -> {
			exchange.getIn().setHeader(Exchange.HTTP_METHOD, "GET");
		};
	}

	private Processor prepareForHttpPostRequest() {
		return (exchange) -> {
			exchange.getIn().setHeader(Exchange.HTTP_METHOD, "POST");
		};
	}

	private Exchange aggregate(Exchange voucher, Exchange payload) {
		if (payload == null) { //means timeout happens on pollEnrich
			log.error("PDF file is not presented for {}", voucher.getIn().getHeader(Exchange.FILE_PATH));
			throw new IllegalArgumentException("PDF file is not presented");
		}
		final ContentType contentType = ContentType.create(voucher.getIn().getHeader("mime", String.class));
		final HttpEntity resultEntity = MultipartEntityBuilder
				.create()
				.addTextBody("comment", "TODO")
				.addTextBody("uploadType", "NewDoc")
				.addBinaryBody("uploadedDocument", payload.getIn().getBody(InputStream.class), contentType, voucher.getIn().getHeader("file", String.class))
				.build();

		payload.getIn().setHeaders(voucher.getIn().getHeaders());
		payload.getIn().setHeader(Exchange.CONTENT_TYPE, resultEntity.getContentType().getValue());
		try {
			final ByteArrayOutputStream baos = new ByteArrayOutputStream();
			resultEntity.writeTo(baos);
			payload.getIn().setBody(new ByteArrayInputStream(baos.toByteArray()));
		} catch (final IOException ioException) {
			throw new UncheckedIOException(ioException);
		}
		return payload;
	}

	private Processor movePdfToErrorFolder() {
		return (exchange) -> {
			String parent = (String) exchange.getIn().getHeader(Exchange.FILE_PARENT);

			//in case of error on the xml parsing this header does not present
			//todo can we touch pdf files without knowing exactly that the file should be moved to the error location?
			String filename = (String) exchange.getIn().getHeader("file");
			if (filename == null) {
				log.info("No pdf name provided. Using the xml base file name.");
				String xmlFileName = (String) exchange.getIn().getHeader(Exchange.FILE_NAME);
				if (xmlFileName == null) {
					log.info("No xml name provided.");
					return;
				}
				filename = FilenameUtils.removeExtension(xmlFileName) + ".pdf";
			}

			Path source = Paths.get(parent, filename);
			log.info("computed pdf source = {}", source);
			if (!Files.exists(source)) {
				log.warn("No pdf file found: {}", source);
				return;
			}
			Path dest = Paths.get(parent, "error");
			if (!Files.exists(dest)) {
				log.info("creating dir = {}", dest);
				Files.createDirectories(dest);
			}
			Path pdfDes = dest.resolve(source.getFileName());
			log.info("pdf destination = {}", pdfDes);
			try {
				Files.move(source, pdfDes, StandardCopyOption.REPLACE_EXISTING);
			} catch (Exception e) {
				log.error("Error moving pdf file", e);
			}
		};
	}
}
