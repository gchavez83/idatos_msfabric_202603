[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "humanresources department_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"humanresources department" = let
  Source = PostgreSQL.Database("wcalcagno.postgres.database.azure.com", "adventureworks"),
  #"Navigation 1" = Source{[Schema = "humanresources", Item = "department"]}[Data],
  #"Choose columns" = Table.SelectColumns(#"Navigation 1", {"departmentid", "name", "groupname", "modifieddate"})
in
  #"Choose columns";
shared #"humanresources department_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "ed3943de-ba3c-46d9-9f6b-6947697f0090"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "145bdfbe-52c9-4c4c-8220-b834c7938e3c"]}[Data],
  TableNavigation = Navigation_2{[Id = "humanresources_department", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
