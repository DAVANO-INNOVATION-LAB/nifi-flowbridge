import java.nio.file.*;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.dsl.yaml.YamlRoutesBuilderLoader;
import org.apache.camel.support.ResourceHelper;
import com.typesafe.config.ConfigFactory;

public class NativeImportProof {
  static void parseCamel(String text) throws Exception {
    try (DefaultCamelContext context = new DefaultCamelContext()) {
      YamlRoutesBuilderLoader loader = new YamlRoutesBuilderLoader();
      loader.setCamelContext(context);
      context.addRoutes(loader.loadRoutesBuilder(ResourceHelper.fromString("routes.yaml",text)));
      if(context.getRouteDefinitions().size()!=1) throw new IllegalStateException("Expected one route");
      // Resolve the actual AWS component URI options without starting consumers or context.
      context.getEndpoint(context.getRouteDefinitions().get(0).getInput().getUri());
      for(var step: context.getRouteDefinitions().get(0).getOutputs()) {
        if(step instanceof org.apache.camel.model.ToDefinition to) context.getEndpoint(to.getUri());
      }
    }
  }
  public static void main(String[] args) throws Exception {
    Path fixtures=Path.of(args[0]); int camel=0, sea=0;
    try(var files=Files.list(fixtures)) {
      for(Path file:files.sorted().toList()) {
        if(file.getFileName().toString().startsWith(".")) continue;
        String text=Files.readString(file);
        if(file.getFileName().toString().startsWith("camel-")) {parseCamel(text);camel++;}
        if(file.getFileName().toString().startsWith("seatunnel-")) {
          var config=ConfigFactory.parseString(text).resolve();
          if(!config.getString("source.S3File.file_format_type").equals("binary")) throw new IllegalStateException("Not binary");
          sea++;
        }
      }
    }
    if(camel!=3 || sea!=3) throw new IllegalStateException("Expected three lanes each");
    boolean malformedRejected=false, unknownStepRejected=false, unknownOptionRejected=false;
    try {ConfigFactory.parseString("{ broken: [");} catch(Exception e){malformedRejected=true;}
    try {parseCamel("- from:\n    uri: direct:test\n    steps:\n      - notARealStep: value\n");} catch(Exception e){unknownStepRejected=true;}
    try {parseCamel("- from:\n    uri: aws2-s3://example?notARealOption=true\n    steps:\n      - to: direct:end\n");} catch(Exception e){unknownOptionRejected=true;}
    if(!malformedRejected || !unknownStepRejected || !unknownOptionRejected) throw new IllegalStateException("Negative controls failed");
    System.out.println("PROOF Camel 4.18.0 native YAML DSL: 3 routes imported; unknown EIP and AWS endpoint option rejected; context never started.");
    if(!ConfigFactory.parseString("source { NotAConnector { nonsense=true } }").hasPath("source.NotAConnector.nonsense")) throw new IllegalStateException("Syntax-only control unexpected");
    System.out.println("PROOF Typesafe Config 1.4.3: 3 SeaTunnel JSON configurations syntax parsed; malformed syntax rejected. NOT SeaTunnel connector validation.");
  }
}
