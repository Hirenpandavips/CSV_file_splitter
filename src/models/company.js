module.exports = (dbConnection, Sequelize) => {
  const company = dbConnection.define(
    'company',
    {
      id: {
        type: Sequelize.INTEGER,
        primaryKey: true,
        autoIncrement: true,
        allowNull: false,
      },
      name: {
        type: Sequelize.STRING,
        unique: true,
        allowNull: false,
      },
      industry: {
        type: Sequelize.STRING,
        allowNull: true
      },
      website: {
        type: Sequelize.STRING,
        allowNull: true
      },
      address: {
        type: Sequelize.TEXT,
        allowNull: true
      },
      origin: {
        type: Sequelize.TEXT,
        allowNull: true
      },
      processingStatus: {
        type: Sequelize.ENUM('pending', 'completed', 'failed'),
        defaultValue: 'pending',
        allowNull: false
      },
    },
    {
      timestamps: true,
      indexes: [
        {
          unique: true,
          fields: ['name'],
        },
      ],
    }
  )
  company.associate = (models) => {
    company.hasMany(models.contact, {
      foreignKey: 'companyId',
      onDelete: 'CASCADE',
    })
  }
  return company
}
