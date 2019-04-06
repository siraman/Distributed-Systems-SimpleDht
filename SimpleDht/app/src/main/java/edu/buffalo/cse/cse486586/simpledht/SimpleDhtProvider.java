package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDhtProvider extends ContentProvider {
    static final int SERVER_PORT = 10000;
    static final String REMOTE_PORT0 = "11108";
    static final String NODE_JOIN = "NODE_JOIN";
    public static final String NODE_JOINED = "NODE_JOINED";
    private static final String NODE_JOINED_REPLY = "NODE_JOINED_REPLY";
    private static final String NODE_UPDATE = "NODE_UPDATE";
    private static final String NODE_UPDATE_REPLY = "NODE_UPDATE_REPLY";
    private static final String REQUEST_NODE_INFO = "REQUEST_NODE_INFO";
    private static final String REQUEST_NODE_INFO_REPLY = "REQUEST_NODE_INFO_REPLY";
    private static final String ACK_MESSAGE = "ACK_MESSAGE";
    static final String INTRODUCER = REMOTE_PORT0;
    private static final String INSERT = "INSERT";
    private static final String INSERT_TO_NODE = "INSERT_TO_NODE";
    private static final String QUERY_NODE = "QUERY_NODE";
    private static final String QUERY_NODE_REPLY = "QUERY_NODE_REPLY";
    private static final String DELTE_NODE = "DELTE_NODE";
    private static final String DELETE_NODE_REPLY = "DELETE_NODE_REPLY";
    private static String MY_PORT;
    DatabaseHelper dbHelper;
    public static Node myNode;
    private static Uri mUri;
    private static String KEY = "key";
    private static String VALUE = "value";
    SQLiteDatabase dbWriter;
    SQLiteDatabase dbReader;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        String[] selectionItems = selection.split(":");
        if(iAmTheOnlyNode()){
            if(selection.equals("@") || selection.equals("*")){
                return dbWriter.delete(SimpleDhtContract.MessageEntry.TABLE_NAME,null, null);
            }
            else {
                String selectionItem = SimpleDhtContract.MessageEntry.COLUMN_KEY + " = ?";
                selectionArgs = new String[]{selection};
                return dbWriter.delete(SimpleDhtContract.MessageEntry.TABLE_NAME,selectionItem, selectionArgs);
            }
        }
        else if(selectionItems[0].equals("@") || selectionItems[0].equals("*")){
            return deleteAllRowsBasedOnSelection(selectionItems);
        }
        else {
            return deleteRequestedSelection(selectionItems);
        }
    }

    private int deleteAllRowsBasedOnSelection(String[] selectionItems) {
        String selection = selectionItems[0];
        int deleteRows = 0;
        if(selection.equals("@")){
            deleteRows = dbReader.delete(SimpleDhtContract.MessageEntry.TABLE_NAME,null, null);
        }
        else if(selection.equals("*")){
            String messageToSend = "";
            String queryRequester;
            String toPort = myNode.getNodeSuccessorPort();
            if(selectionItems.length > 1)
                queryRequester = selectionItems[1];
            else
                queryRequester = myNode.getNodePort();

            if (myNode.getNodeSuccessorPort().equals(queryRequester)) {
                deleteRows = dbReader.delete(SimpleDhtContract.MessageEntry.TABLE_NAME,null, null);
            }
            else {
                messageToSend = selectionItems[0];
                deleteRows = queryFromNextNodeToDelete(queryRequester, messageToSend, toPort);
            }
        }
        return deleteRows;
    }

    private int deleteRequestedSelection(String[] selection) {
        String selectionItems = selection[0];
        String selectionItem = SimpleDhtContract.MessageEntry.COLUMN_KEY + " = ?";
        String[] selectionArgs = new String[]{selectionItems};
        String hashedKey = genHash(selectionItems);
        if(isInMyRegion(hashedKey)){
            return dbWriter.delete(SimpleDhtContract.MessageEntry.TABLE_NAME,selectionItem,selectionArgs);
        }
        else {
            String queryRequester;
            if(selection.length > 1)
                queryRequester = selection[1];
            else
                queryRequester = myNode.getNodePort();
            String toPort = myNode.getNodeSuccessorPort();
            String messageToSend = selection[0];
            return queryFromNextNodeToDelete(queryRequester, messageToSend, toPort);
        }
    }

    private int queryFromNextNodeToDelete(String queryRequester, String message, String toPort) {
        int deleteRows;
        while (true){
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(toPort));
                PrintStream printStream = new PrintStream(socket.getOutputStream());
                String messageToSend = DELTE_NODE +":" + message + ":" + queryRequester;
                printStream.println(messageToSend);
                printStream.flush();

                InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                String messageReceived = bufferedReader.readLine();
                String[] msg = messageReceived.split(":");
                if(msg[0].equals(DELETE_NODE_REPLY) && msg[1].equals("*")){
                    deleteRows = dbWriter.delete(SimpleDhtContract.MessageEntry.TABLE_NAME,null,null);
                    socket.close();
                    break;
                }
                else if (msg[0].equals(DELETE_NODE_REPLY)) {
                    if(msg.length > 2)
                        deleteRows = Integer.parseInt(msg[2]);
                    else
                        deleteRows = 0;
                    socket.close();
                    break;
                }
            }
            catch (IOException e){
                e.printStackTrace();
            }
        }
        return deleteRows;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        try {
            String hashedKey = genHash(values.getAsString(KEY));
            String key = values.getAsString(KEY);
            String value = values.getAsString(VALUE);
            if(iAmTheOnlyNode()){
                dbWriter.insertWithOnConflict(SimpleDhtContract.MessageEntry.TABLE_NAME,null,
                        values,SQLiteDatabase.CONFLICT_REPLACE);
            }
            else if(isInMyRegion(hashedKey)){
                dbWriter.insertWithOnConflict(SimpleDhtContract.MessageEntry.TABLE_NAME,null,
                        values,SQLiteDatabase.CONFLICT_REPLACE);
            }
            else{
                String toPort = myNode.getNodeSuccessorPort();
                String messageToSend = key + ":" + value;
                sendMessage(messageToSend, INSERT, toPort);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private boolean iAmTheOnlyNode() {
        return myNode.getNodeSuccessor().isEmpty() && myNode.getNodePredecessor().isEmpty();
    }

    public void insertToMySpace(ContentValues values){
        dbWriter.insertWithOnConflict(SimpleDhtContract.MessageEntry.TABLE_NAME,null,
                values,SQLiteDatabase.CONFLICT_REPLACE);
    }

    @Override
    public boolean onCreate() {
        dbHelper = new DatabaseHelper(getContext());
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
        dbWriter = dbHelper.getWritableDatabase();
        dbReader = dbHelper.getReadableDatabase();
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        MY_PORT = myPort;
        myNode = new Node();
        try {
            myNode.setNodeId(genHash(portStr));
            myNode.setNodePort(myPort);

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

            if (!myNode.getNodePort().equals(INTRODUCER)) {
                String messageToSend = myNode.getNodeId() + ":" + MY_PORT;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messageToSend,
                        NODE_JOIN);
            }
        }
        catch (IOException io) {
            io.printStackTrace();
        }
        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        Cursor cursor = null;
        String[] selectionItems = selection.split(":");
        if(iAmTheOnlyNode()){
            if(selection.equals("@") || selection.equals("*")){
                cursor = dbReader.query(SimpleDhtContract.MessageEntry.TABLE_NAME,null, null, null,
                        null,null,null);
                return cursor;
            }
            else {
                String selectionItem = SimpleDhtContract.MessageEntry.COLUMN_KEY + " = ?";
                selectionArgs = new String[]{selection};
                projection = new String[]{KEY,VALUE};
                cursor = dbReader.query(SimpleDhtContract.MessageEntry.TABLE_NAME,projection, selectionItem, selectionArgs,
                        null,null,sortOrder);
                return cursor;
            }
        }
        else if(selectionItems[0].equals("@") || selectionItems[0].equals("*")){
            return getAllRowsBasedOnSelection(selectionItems);
        }
        else {
            return getRequestedSelection(selectionItems);
        }
    }

    private Cursor getRequestedSelection(String[] selection) {
        String selectionItems = selection[0];
        Cursor cursor;
        String selectionItem = SimpleDhtContract.MessageEntry.COLUMN_KEY + " = ?";
        String[] selectionArgs = new String[]{selectionItems};
        String hashedKey = genHash(selectionItems);
        if(isInMyRegion(hashedKey)){
            cursor = dbReader.query(SimpleDhtContract.MessageEntry.TABLE_NAME,null, selectionItem, selectionArgs,
                    null,null,null);
            return cursor;
        }
        else {

            String queryRequester;
            if(selection.length > 1)
                queryRequester = selection[1];
            else
                queryRequester = myNode.getNodePort();
            String toPort = myNode.getNodeSuccessorPort();
            String messageToSend = selection[0];
            return queryFromNextNode(queryRequester, messageToSend, toPort);
        }
    }

    private Cursor queryFromNextNode(String queryRequester, String message, String toPort) {
        MatrixCursor cursor = null;
        Cursor myCursor = null;
        while (true){
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(toPort));
                PrintStream printStream = new PrintStream(socket.getOutputStream());
                String messageToSend = QUERY_NODE +":" + message + ":" + queryRequester;
                printStream.println(messageToSend);
                printStream.flush();

                InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                String messageReceived = bufferedReader.readLine();
                String[] msg = messageReceived.split(":");
                if(msg[0].equals(QUERY_NODE_REPLY) && msg[1].equals("*")){
                    if(msg.length > 2) {
                        cursor = new MatrixCursor(new String[]{KEY,VALUE});
                        myCursor = dbReader.query(SimpleDhtContract.MessageEntry.TABLE_NAME,null, null, null,
                                null,null,null);
                        if(myCursor != null && myCursor.getCount() > 0){
                            myCursor.moveToFirst();
                            while (!myCursor.isAfterLast()) {
                                String key = myCursor.getString(myCursor.getColumnIndex(KEY));
                                String value = myCursor.getString(myCursor.getColumnIndex(VALUE));
                                cursor.newRow().add(KEY,key)
                                        .add(VALUE,value);
                                myCursor.moveToNext();
                            }
                        }
                        String[] content = msg[2].split("==");
                        for (int j = 0; j < content.length; j++) {
                            cursor.newRow().add(KEY, content[j].split(";")[0])
                                            .add(VALUE, content[j].split(";")[1]);
                        }
                    }
                    else {
                        cursor = new MatrixCursor(new String[]{KEY,VALUE});
                        myCursor = dbReader.query(SimpleDhtContract.MessageEntry.TABLE_NAME,null, null, null,
                                null,null,null);
                        if(myCursor != null && myCursor.getCount() > 0){
                            myCursor.moveToFirst();
                            while (!myCursor.isAfterLast()) {
                                String key = myCursor.getString(myCursor.getColumnIndex(KEY));
                                String value = myCursor.getString(myCursor.getColumnIndex(VALUE));
                                cursor.newRow().add(KEY,key)
                                        .add(VALUE,value);
                                myCursor.moveToNext();
                            }
                        }
                    }
                    socket.close();
                    break;
                }
                else if (msg[0].equals(QUERY_NODE_REPLY)) {
                    if(msg.length > 2) {
                        String[] content = msg[2].split("==");
                        cursor = new MatrixCursor(new String[]{KEY,VALUE});
                        for (int i = 0; i < content.length; i++) {
                            cursor.newRow().add(KEY, content[i].split(";")[0])
                                    .add(VALUE, content[i].split(";")[1]);
                        }
                    }
                    socket.close();
                    break;
                }
            }
            catch (IOException e){
                e.printStackTrace();
            }
        }
        return cursor;
    }

    private Cursor getAllRowsBasedOnSelection(String[] selectionItems) {
        Cursor cursor = null;
        String selection = selectionItems[0];

        if(selection.equals("@")){
            cursor = dbReader.query(SimpleDhtContract.MessageEntry.TABLE_NAME,null, null, null,
                    null,null,null);
            return cursor;
        }
        else if(selection.equals("*")){
            String messageToSend = "";
            String queryRequester;
            String toPort = myNode.getNodeSuccessorPort();
            if(selectionItems.length > 1)
                queryRequester = selectionItems[1];
            else
                queryRequester = myNode.getNodePort();

            if (myNode.getNodeSuccessorPort().equals(queryRequester)) {
                cursor = dbReader.query(SimpleDhtContract.MessageEntry.TABLE_NAME,null, null, null,
                        null,null,null);
                return cursor;
            }
            else {
                messageToSend = selectionItems[0];
                cursor = queryFromNextNode(queryRequester, messageToSend, toPort);
            }
        }
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            Socket socket;
            String[] message = msgs[0].split(":");
            String messageType = msgs[1];
            if(messageType.equals(NODE_JOIN)) {
                try {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(INTRODUCER));
                    PrintStream printStream = new PrintStream(socket.getOutputStream());
                    String messageToSend = messageType + ":" + message[0] + ":" + message[1];
                    printStream.println(messageToSend);
                    printStream.flush();
                    socket.setSoTimeout(3000);
                    InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
                    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                    String ackMessage = bufferedReader.readLine();
                    if (ackMessage.equals(ACK_MESSAGE)) {
                        printStream.close();
                        socket.close();
                    }
                }
                catch (NullPointerException ne){
                    ne.printStackTrace();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
            else if(messageType.equals(NODE_JOINED)){
                try {
                    String destinationPort = msgs[2];
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(destinationPort));
                    PrintStream printStream = new PrintStream(socket.getOutputStream());
                    String nodePredecessor = message[0];
                    String nodePredecessorPort = message[1];
                    String nodeSuccessor = message[2];
                    String nodeSuccessorPort = message[3];
                    String messageToSend = NODE_JOINED_REPLY + ":" + nodePredecessor + ":" +
                            nodePredecessorPort + ":" + nodeSuccessor + ":" + nodeSuccessorPort;
                    printStream.println(messageToSend);
                    printStream.flush();

                    InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
                    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                    String ackMessage = bufferedReader.readLine();
                    if (ackMessage.equals(ACK_MESSAGE)) {
                        printStream.close();
                        socket.close();
                    }
                }
                catch (NullPointerException ne){
                    ne.printStackTrace();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
            else if (messageType.equals(NODE_UPDATE)) {
                try {
                    String destinationPort = msgs[2];
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(destinationPort));
                    PrintStream printStream = new PrintStream(socket.getOutputStream());

                    String nodePredecessor = message[0];
                    String nodePredecessorPort = message[1];

                    String messageToSend = NODE_UPDATE_REPLY + ":" + nodePredecessor + ":" +
                            nodePredecessorPort;
                    printStream.println(messageToSend);
                    printStream.flush();

                    InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
                    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                    String ackMessage = bufferedReader.readLine();
                    if (ackMessage.equals(ACK_MESSAGE)) {
                        printStream.close();
                        socket.close();
                    }
                }
                catch (NullPointerException ne){
                    ne.printStackTrace();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
            else if(messageType.equals(REQUEST_NODE_INFO)){
                try{
                    String destinationPort = msgs[2];
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(destinationPort));
                    PrintStream printStream = new PrintStream(socket.getOutputStream());

                    String messageToSend = REQUEST_NODE_INFO_REPLY + ":" + message[0] + ":" + message[1];
                    printStream.println(messageToSend);
                    printStream.flush();

                    InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
                    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                    String ackMessage = bufferedReader.readLine();
                    if (ackMessage.equals(ACK_MESSAGE)) {
                        printStream.close();
                        socket.close();
                    }
                }
                catch (NullPointerException ne){
                    ne.printStackTrace();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
            else if(messageType.equals(INSERT)){
                try {
                    String destinationPort = msgs[2];
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(destinationPort));
                    PrintStream printStream = new PrintStream(socket.getOutputStream());
                    String messageToSend = INSERT_TO_NODE + ":" + message[0] + ":" + message[1];
                    printStream.println(messageToSend);
                    printStream.flush();

                    InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
                    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                    String ackMessage = bufferedReader.readLine();
                    if (ackMessage.equals(ACK_MESSAGE)) {
                        printStream.close();
                        socket.close();
                    }
                }
                catch (NullPointerException ne){
                    ne.printStackTrace();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return null;
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {
                while (true) {
                    Socket socket = serverSocket.accept();
                    InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
                    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                    String message = bufferedReader.readLine();

                    String[] messageParts = message.split(":");
                    if(messageParts[0].equals(NODE_JOIN)) {
                        if(myNode.getNodeSuccessor().isEmpty() ||
                                myNode.getNodePredecessor().isEmpty()){
                            myNode.setNodeSuccessor(messageParts[1]);
                            myNode.setNodeSuccessorPort(messageParts[2]);
                            myNode.setNodePredecessor(messageParts[1]);
                            myNode.setNodePredecessorPort(messageParts[2]);
                            String nodePredecessor = myNode.getNodeId();
                            String nodePredecessorPort = myNode.getNodePort();
                            String nodeSuccessor = myNode.getNodeId();
                            String nodeSuccessorPort = myNode.getNodePort();
                            String toPort = messageParts[2];

                            String messageToSend = nodePredecessor + ":" +
                                    nodePredecessorPort + ":" + nodeSuccessor + ":" + nodeSuccessorPort;
                            sendMessage(messageToSend, NODE_JOINED, toPort);
                        }
                        else {
                            if(isInMyRegion(messageParts[1])){
                                String nodePredecessor = myNode.getNodePredecessor();
                                String nodePredecessorPort = myNode.getNodePredecessorPort();
                                String nodeSuccessor = myNode.getNodeId();
                                String nodeSuccessorPort = myNode.getNodePort();
                                String toPort = messageParts[2];

                                String messageToSend = nodePredecessor + ":" +
                                        nodePredecessorPort + ":" + nodeSuccessor + ":" + nodeSuccessorPort;

                                myNode.setNodePredecessor(messageParts[1]);
                                myNode.setNodePredecessorPort(messageParts[2]);
                                sendMessage(messageToSend, NODE_JOINED, toPort);
                            }
                            else {
                                String requestInfoFor = messageParts[1];
                                String toPort = myNode.getNodeSuccessorPort();
                                String messageToSend = requestInfoFor + ":" + messageParts[2];
                                sendMessage(messageToSend,REQUEST_NODE_INFO,toPort);
                            }
                        }
                    }
                    else if (messageParts[0].equals(NODE_JOINED_REPLY)){
                        myNode.setNodePredecessor(messageParts[1]);
                        myNode.setNodePredecessorPort(messageParts[2]);
                        myNode.setNodeSuccessor(messageParts[3]);
                        myNode.setNodeSuccessorPort(messageParts[4]);

                        String newSuccessor = myNode.getNodeId();
                        String newSuccessorPort = myNode.getNodePort();
                        String toPort = myNode.getNodePredecessorPort();
                        String messageToSend = newSuccessor + ":" + newSuccessorPort;
                        sendMessage(messageToSend,NODE_UPDATE,toPort);
                    }
                    else if(messageParts[0].equals(NODE_UPDATE_REPLY)) {
                        myNode.setNodeSuccessor(messageParts[1]);
                        myNode.setNodeSuccessorPort(messageParts[2]);
                    }
                    else if (messageParts[0].equals(REQUEST_NODE_INFO_REPLY)){
                        if(isInMyRegion(messageParts[1])){
                            String newPredecessor = myNode.getNodePredecessor();
                            String newPredecessorPort = myNode.getNodePredecessorPort();
                            String newSuccessor = myNode.getNodeId();
                            String newSuccessorPort = myNode.getNodePort();
                            String messageToSend = newPredecessor + ":" + newPredecessorPort+ ":" +
                                    newSuccessor + ":" + newSuccessorPort;
                            myNode.setNodePredecessor(messageParts[1]);
                            myNode.setNodePredecessorPort(messageParts[2]);
                            sendMessage(messageToSend,NODE_JOINED,messageParts[2]);
                        }
                        else {
                            String requestInfoFrom = messageParts[1];
                            String toPort = myNode.getNodeSuccessorPort();
                            String messageToSend = requestInfoFrom + ":" + messageParts[2];
                            sendMessage(messageToSend,REQUEST_NODE_INFO,toPort);
                        }
                    }
                    else if(messageParts[0].equals(INSERT_TO_NODE)){
                        if(isInMyRegion(genHash(messageParts[1]))){
                            ContentValues contentValues = new ContentValues();
                            contentValues.put(KEY,messageParts[1]);
                            contentValues.put(VALUE,messageParts[2]);
                            insertToMySpace(contentValues);
                        }
                        else {
                            String toPort = myNode.getNodeSuccessorPort();
                            String messageToSend = messageParts[1] + ":" + messageParts[2];
                            sendMessage(messageToSend, INSERT, toPort);
                        }
                    }
                    else if(messageParts[0].equals(QUERY_NODE)){
                        String selection = messageParts[1] + ":" + messageParts[2];
                        Cursor cursor = query(null, new String[]{KEY,VALUE},selection,null,null);
                        String messageToSend = "";
                        if(cursor != null && cursor.getCount() > 0){
                            cursor.moveToFirst();
                            while (!cursor.isAfterLast()) {
                                String key = cursor.getString(cursor.getColumnIndex(KEY));
                                String value = cursor.getString(cursor.getColumnIndex(VALUE));
                                if(messageToSend.isEmpty()) {
                                    messageToSend += QUERY_NODE_REPLY + ":" + messageParts[1]+ ":" + key+";"+ value;
                                }
                                else
                                    messageToSend += "=="+key+";"+value;
                                cursor.moveToNext();
                            }
                        }
                        else {
                            messageToSend = QUERY_NODE_REPLY + ":" + messageParts[1];
                        }
                        PrintStream printStream = new PrintStream(socket.getOutputStream());
                        printStream.println(messageToSend);
                        printStream.flush();
                        if(cursor!=null)
                            cursor.close();
                        socket.close();
                        continue;
                    }
                    else if(messageParts[0].equals(DELTE_NODE)){
                        String selection = messageParts[1] + ":" + messageParts[2];
                        int deletedRows = delete(null, selection,null);
                        String messageToSend = "";
                        if(deletedRows > 0){
                            messageToSend = DELETE_NODE_REPLY + ":" + messageParts[1] + ":" + deletedRows;
                        }
                        else
                            messageToSend = DELETE_NODE_REPLY + ":" + messageParts[1];
                        PrintStream printStream = new PrintStream(socket.getOutputStream());
                        printStream.println(messageToSend);
                        printStream.flush();
                        socket.close();
                        continue;
                    }
                    PrintStream printStream = new PrintStream(socket.getOutputStream());
                    printStream.println(ACK_MESSAGE);
                    printStream.flush();
                    socket.close();
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }
    private boolean isInMyRegion(String currentNode) {
        if(currentNode.compareTo(myNode.getNodePredecessor()) > 0 &&
            currentNode.compareTo(myNode.getNodeId()) <= 0)
            return true;
        if(myNode.getNodePredecessor().compareTo(myNode.getNodeId()) > 0 &&
            currentNode.compareTo(myNode.getNodePredecessor()) > 0 &&
            currentNode.compareTo(myNode.getNodeId()) > 0)
            return true;
        if(myNode.getNodePredecessor().compareTo(myNode.getNodeId()) > 0 &&
                currentNode.compareTo(myNode.getNodePredecessor()) < 0 &&
                currentNode.compareTo(myNode.getNodeId()) <= 0)
            return true;
        return false;
    }

    private void sendMessage(String message, String messageType, String toPort) {
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, messageType, toPort);
    }

    private String genHash(String input) {
        MessageDigest sha1 = null;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
}
