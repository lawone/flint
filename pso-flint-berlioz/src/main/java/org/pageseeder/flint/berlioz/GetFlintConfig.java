/*
 * Decompiled with CFR 0_110.
 * 
 * Could not load the following classes:
 *  org.pageseeder.berlioz.BerliozException
 *  org.pageseeder.berlioz.Beta
 *  org.pageseeder.berlioz.content.ContentGenerator
 *  org.pageseeder.berlioz.content.ContentRequest
 *  org.pageseeder.berlioz.util.ISO8601
 *  org.pageseeder.flint.api.Index
 *  org.pageseeder.xmlwriter.XMLWriter
 */
package org.pageseeder.flint.berlioz;

import java.io.File;
import java.io.IOException;

import org.pageseeder.berlioz.BerliozException;
import org.pageseeder.berlioz.Beta;
import org.pageseeder.berlioz.content.ContentGenerator;
import org.pageseeder.berlioz.content.ContentRequest;
import org.pageseeder.berlioz.util.ISO8601;
import org.pageseeder.flint.berlioz.model.FlintConfig;
import org.pageseeder.flint.berlioz.model.IndexMaster;
import org.pageseeder.flint.berlioz.util.FileFilters;
import org.pageseeder.xmlwriter.XMLWriter;

@Beta
public final class GetFlintConfig implements ContentGenerator {

  public void process(ContentRequest req, XMLWriter xml) throws BerliozException, IOException {
    FlintConfig config = FlintConfig.get();
    File directory = config.getRootDirectory();
    xml.openElement("flint-config");
    xml.attribute("directory", directory.getName().equals("index") ? "index" : directory.getName());
    xml.attribute("class", config.getClass().getName());
    if (directory.exists() && directory.isDirectory()) {
      for (File f : directory.listFiles(FileFilters.getFolders())) {
        this.toBasicIndexXML(xml, f);
      }
    }
    xml.closeElement();
  }

  private void toBasicIndexXML(XMLWriter xml, File index) throws IOException {
    xml.openElement("index");
    xml.attribute("name", index.getName());
    IndexMaster master = FlintConfig.get().getMaster(index.getName());
    long modified = master == null ? -1 : master.getIndex().getIndexIO().getLastTimeUsed();
    boolean exists = modified > 0;
    xml.attribute("exists", Boolean.toString(exists));
    if (exists) {
      xml.attribute("modified", ISO8601.format((long)modified, (ISO8601)ISO8601.DATETIME));
    }
    xml.closeElement();
  }
}

