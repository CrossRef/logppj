package logpp;

class Formats {
  // To indicate that it wasn't supplied.
  static String UNKNOWN_DOMAIN = "unknown.special";

  // To indicate that it was a local file.
  static String LOCAL_DOMAIN = "local.special";

  // Various codes.
  static String CODE_HTTP = "H"; // for HTTP protocol
  static String CODE_HTTPS = "S"; // for HTTPS protocol
  static String CODE_FTP = "F"; // for FTP protocol.
  static String CODE_FILE = "L"; // for file:// protocol.
  static String CODE_UNKNOWN = "U"; // for unknown protocol, but domain supplied
  static String CODE_NO_INFO = "N"; // for no information.
  static String CODE_WEIRD = "W"; // for weird (e.g. readcube)
}