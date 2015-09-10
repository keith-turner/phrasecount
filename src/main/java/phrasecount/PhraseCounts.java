package phrasecount;

public class PhraseCounts {
  //number of documents a phrase was seen in
  public final long documents;
  //total times a phrase was seen in all documents
  public final long total;

  public PhraseCounts(){
    documents = 0;
    total = 0;
  }

  public PhraseCounts(long docs, long total) {
    this.documents = docs;
    this.total = total;
  }

  public PhraseCounts add(PhraseCounts other) {
    return new PhraseCounts(this.documents + other.documents, this.total + other.total);
  }
}
