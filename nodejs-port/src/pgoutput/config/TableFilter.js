export class TableFilter {
  static includeAll() {
    return new TableFilter(() => true);
  }

  static exclude(...tableNames) {
    const excludeSet = new Set(tableNames);
    return new TableFilter((table) => !excludeSet.has(table));
  }

  static include(...tableNames) {
    const includeSet = new Set(tableNames);
    return new TableFilter((table) => includeSet.has(table));
  }

  constructor(filterFn) {
    this.filterFn = filterFn;
  }

  shouldInclude(table) {
    return this.filterFn(table);
  }
}