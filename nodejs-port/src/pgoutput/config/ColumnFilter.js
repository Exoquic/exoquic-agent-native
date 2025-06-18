export class ColumnFilter {
  static includeAll() {
    return new ColumnFilter(() => true);
  }

  static exclude(...columnNames) {
    const excludeSet = new Set(columnNames);
    return new ColumnFilter((table, column) => !excludeSet.has(column));
  }

  static include(...columnNames) {
    const includeSet = new Set(columnNames);
    return new ColumnFilter((table, column) => includeSet.has(column));
  }

  constructor(filterFn) {
    this.filterFn = filterFn;
  }

  shouldInclude(table, column) {
    return this.filterFn(table, column);
  }
}