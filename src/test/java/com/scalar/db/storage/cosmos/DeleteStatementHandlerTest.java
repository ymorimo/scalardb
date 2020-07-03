package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.CosmosScripts;
import com.azure.cosmos.CosmosStoredProcedure;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosStoredProcedureRequestOptions;
import com.azure.cosmos.models.CosmosStoredProcedureResponse;
import com.azure.cosmos.models.PartitionKey;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Operation;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DeleteStatementHandlerTest {
  private static final String ANY_KEYSPACE_NAME = "keyspace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";

  private DeleteStatementHandler handler;
  private String id;
  private PartitionKey cosmosPartitionKey;
  @Mock private CosmosClient client;
  @Mock private CosmosDatabase database;
  @Mock private CosmosContainer container;
  @Mock private TableMetadataManager metadataManager;
  @Mock private TableMetadata metadata;
  @Mock private CosmosItemResponse response;
  @Mock private CosmosScripts cosmosScripts;
  @Mock private CosmosStoredProcedure storedProcedure;
  @Mock private CosmosStoredProcedureResponse spResponse;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    handler = new DeleteStatementHandler(client, metadataManager);
    when(client.getDatabase(anyString())).thenReturn(database);
    when(database.getContainer(anyString())).thenReturn(container);

    when(metadataManager.getTableMetadata(any(Operation.class))).thenReturn(metadata);
    when(metadata.getPartitionKeyNames())
        .thenReturn(new HashSet<String>(Arrays.asList(ANY_NAME_1)));
    when(metadata.getKeyNames()).thenReturn(Arrays.asList(ANY_NAME_1, ANY_NAME_2));
  }

  private Delete prepareDelete() {
    Key partitionKey = new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    Key clusteringKey = new Key(new TextValue(ANY_NAME_2, ANY_TEXT_2));
    id = ANY_TEXT_1 + ":" + ANY_TEXT_2;
    cosmosPartitionKey = new PartitionKey(ANY_TEXT_1);
    Delete del =
        new Delete(partitionKey, clusteringKey)
            .forNamespace(ANY_KEYSPACE_NAME)
            .forTable(ANY_TABLE_NAME);
    return del;
  }

  @Test
  public void handle_DeleteWithoutConditionsGiven_ShouldCallDeleteItem() {
    // Arrange
    when(container.deleteItem(
            anyString(), any(PartitionKey.class), any(CosmosItemRequestOptions.class)))
        .thenReturn(response);
    Delete delete = prepareDelete();

    // Act Assert
    assertThatCode(
            () -> {
              handler.handle(delete);
            })
        .doesNotThrowAnyException();

    // Assert
    verify(container)
        .deleteItem(eq(id), eq(cosmosPartitionKey), any(CosmosItemRequestOptions.class));
  }

  @Test
  public void handle_DeleteWithoutConditionsCosmosExceptionThrown_ShouldThrowExecutionException() {
    // Arrange
    CosmosException toThrow = mock(CosmosException.class);
    doThrow(toThrow)
        .when(container)
        .deleteItem(anyString(), any(PartitionKey.class), any(CosmosItemRequestOptions.class));

    Delete delete = prepareDelete();

    // Act Assert
    assertThatThrownBy(
            () -> {
              handler.handle(delete);
            })
        .isInstanceOf(ExecutionException.class)
        .hasCause(toThrow);
  }

  @Test
  public void handle_DeleteWithConditionsGiven_ShouldCallStoredProcedure() {
    // Arrange
    when(container.getScripts()).thenReturn(cosmosScripts);
    when(cosmosScripts.getStoredProcedure(anyString())).thenReturn(storedProcedure);
    when(storedProcedure.execute(any(List.class), any(CosmosStoredProcedureRequestOptions.class)))
        .thenReturn(spResponse);
    when(spResponse.getResponseAsString()).thenReturn("true");

    Delete delete = prepareDelete().withCondition(new DeleteIfExists());
    String query = handler.makeConditionalQuery(delete);

    // Act Assert
    assertThatCode(
            () -> {
              handler.handle(delete);
            })
        .doesNotThrowAnyException();

    // Assert
    verify(cosmosScripts).getStoredProcedure("deleteIf.js");
    ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
    verify(storedProcedure)
        .execute(captor.capture(), any(CosmosStoredProcedureRequestOptions.class));
    assertThat((String) captor.getValue().get(0)).isEqualTo(query);
  }

  @Test
  public void handle_DeleteWithConditionsReturnFalseResponse_ShouldThrowNoMutationException() {
    // Arrange
    when(container.getScripts()).thenReturn(cosmosScripts);
    when(cosmosScripts.getStoredProcedure(anyString())).thenReturn(storedProcedure);
    when(storedProcedure.execute(any(List.class), any(CosmosStoredProcedureRequestOptions.class)))
        .thenReturn(spResponse);
    when(spResponse.getResponseAsString()).thenReturn("false");

    Delete delete = prepareDelete().withCondition(new DeleteIfExists());

    // Act Assert
    assertThatThrownBy(
            () -> {
              handler.handle(delete);
            })
        .isInstanceOf(NoMutationException.class);
  }

  @Test
  public void handle_DeleteWithConditionCosmosExceptionThrown_ShouldThrowExecutionException() {
    // Arrange
    when(container.getScripts()).thenReturn(cosmosScripts);
    when(cosmosScripts.getStoredProcedure(anyString())).thenReturn(storedProcedure);
    CosmosException toThrow = mock(CosmosException.class);
    doThrow(toThrow)
        .when(storedProcedure)
        .execute(any(List.class), any(CosmosStoredProcedureRequestOptions.class));

    Delete delete = prepareDelete().withCondition(new DeleteIfExists());

    // Act Assert
    assertThatThrownBy(
            () -> {
              handler.handle(delete);
            })
        .isInstanceOf(ExecutionException.class)
        .hasCause(toThrow);
  }
}
